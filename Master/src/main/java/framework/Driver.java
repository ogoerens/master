package framework;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.sql.Connection;
import java.sql.SQLException;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import framework.Anonymization.AnonymizationDriver;
import microbench.Queries;
import microbench.Query;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration2.XMLConfiguration;

import util.ResultWriter;
import util.GenericQuery;
import util.Utils;

public class Driver {
  private static final String sourcePath = System.getProperty("user.dir");
  private static final String outputDirectory = sourcePath + "/Results";
  private static final String CardinalityDirectory = sourcePath + "/QueryCardinality";
  private static final int numberWorkers = 1;
  private static final int numberOfQueryExecutions = 3;

  public static void main(String[] args) throws Exception {

    CommandLineParser parser = new DefaultParser();
    Options options = buildOptions();
    CommandLine argsLine = parser.parse(options, args);

    Random rand = new Random();

    // Check if data should be anonymized, i.e. option "a" is set and configuration file is passed
    // as argument.
    if (argsLine.hasOption("a")) {
      AnonymizationDriver ad = new AnonymizationDriver(argsLine.getOptionValue("a"));
      ad.anonymize();
      // Anonym a = new Anonym();
      // a.work();
    }

    // Check if Generator is used. If so, create generator configuration and execute generator. File
    // should be: genconfig.xml.

    if (argsLine.hasOption("g")) {
      XMLConfiguration genConfiguration = Utils.buildXMLConfiguration(argsLine.getOptionValue("g"));
      Generator g = new Generator(rand);
      g.generate(genConfiguration);
    }

    // Check if option "-c" is set. If not, no connection to a database is established and the
    // program will exit.
    if (!argsLine.hasOption("c")) {
      System.out.println(
          "No Connection was established as argument -c was not set. "
              + "The framework will not be able perform any further operations");
      System.exit(0);
    }

    // Option "-c" is set. Create DB connection configuration  from configuration file.
    String dbConfigFile = argsLine.getOptionValue("c");
    XMLConfiguration dbConfiguration = Utils.buildXMLConfiguration(dbConfigFile);
    DatabaseConfiguration config = new DatabaseConfiguration(dbConfiguration);
    config.init();

    // Connect to the database.
    try {
      Connection conn = config.makeConnection();
      if (conn != null) {
        System.out.println("Connected.");
      }

      if (argsLine.hasOption("d")) {
        ArrayList<Query> qList = Query.QueryGenerator.generateDropQueries(Queries.tables, "table");
        for (Query q : qList) {
          try {
            q.update(conn);
          } catch (SQLServerException e) {
            System.err.println(
                q.query_stmt
                    + " failed because the table does not exist or you do not have permission.");
          }
        }
        System.out.println("Drop was performed. Exiting");
        System.exit(0);
      }

      // Check if DataManager is used. If so, create DM configuration and execute DM. File should
      // be: manageconfig.xml.
      // DataManager is used to create/update the tables with generated data from files.

      if (argsLine.hasOption("dm")) {
        XMLConfiguration manConfiguration =
            Utils.buildXMLConfiguration(argsLine.getOptionValue("dm"));
        DataManager dm = new DataManager(conn);
        dm.manage(manConfiguration);
      }

      // Create queries from QueryString and add to transaction queue.
      // TODO check at what qid query generation starts! And clean up this mess.

      ArrayList<String> qIDtoqNameS = new ArrayList<>();
      ArrayList<QueryBool> transactionQueue = new ArrayList<>();

      // Check if the queries of the microbench all have a name assigned to them.
      if (Queries.queryList.length != Queries.queryListNames.length) {
        throw new Exception("Amount of queries and querynames do not overlap");
      }

      // Add queries to transactionqueue. Queries are only timed, if they are equal to the previous
      // query.
      int j = 0;
      String prevQuery = "";
      for (String query : Queries.queryList) {
        for (int i = 0; i < numberOfQueryExecutions; i++) {
          qIDtoqNameS.add(Queries.queryListNames[j]);
          if (prevQuery.equals(query)) {
            transactionQueue.add(
                new QueryBool(new Query(query, Queries.queryListNames[j], true), true));
          } else {
            transactionQueue.add(
                new QueryBool(new Query(query, Queries.queryListNames[j], true), false));
            prevQuery = query;
          }
        }
        j++;
      }

      // Could also define each Query as its own class.
      // Q0 q0 = new Q0();

      // Execute the Queries.
      Worker w = new Worker(conn, transactionQueue, rand, numberWorkers);
      w.work(config.getDatabase());

      // Close database connection.

      // Store cardinalities in a file.
      Files.createDirectories(Paths.get(CardinalityDirectory));
      Path p = Paths.get(CardinalityDirectory + "/" + "queryCardinalitiesOverview.txt");
      Files.deleteIfExists(p);
      HashMap<String, Integer> qNameToCardinality = w.getCardinalities();
      Utils.writeMapToFile(
          qNameToCardinality,
          CardinalityDirectory + "/" + "queryCardinalitiesOverview.txt",
          " : ",
          "\n");

      // For each query, group the execution per query Name. Compute for each of these groups the
      // statistics.
      LinkedHashMap<String, LatencyRecord> latencyRecordPerQueryName =
          w.getLatencyRecord().groupQueriesPerName(qIDtoqNameS);
      LinkedHashMap<String, Statistics> statsPerQueryName = new LinkedHashMap<>();
      for (Map.Entry<String, LatencyRecord> entry : latencyRecordPerQueryName.entrySet()) {
        statsPerQueryName.put(
            entry.getKey(), Statistics.computeStatistics(entry.getValue().getLatenciesAsArray()));
      }

      // Stats for all queries together.
      Statistics stats = Statistics.computeStatistics(w.getLatencyRecord().getLatenciesAsArray());
      statsPerQueryName.put("Overall", stats);

      ArrayList<String> statAttributes = new ArrayList<>();
      statAttributes.add("Average");
      statAttributes.add("Minimum");
      statAttributes.add("75thPercentile");
      statAttributes.add("90thPercentile");
      // Store the statistics for each query group in the overview file.
      String resultOverviewFile = "overview.csv";
      StringBuilder stringBuilderStats = new StringBuilder();
      stringBuilderStats.append(
          "Queries, Returned rows, Average time(us), Minimum time, 75thPercentile(us), 90thPercentile(us) \n");
      for (Map.Entry<String, Statistics> entry : statsPerQueryName.entrySet()) {
        System.out.println(entry.getKey() + " : " + entry.getValue().getAverage());
        stringBuilderStats.append(
            entry.getKey()
                + ","
                + qNameToCardinality.get(entry.getKey())
                + ","
                + entry.getValue().print(statAttributes));
      }
      Utils.StrToFile(stringBuilderStats.toString(), resultOverviewFile);

      // Write a result file per Query Name.
      ResultWriter rw = new ResultWriter();
      Files.createDirectories(Paths.get(outputDirectory));
      String resultsFileName = "results.csv";
      for (Map.Entry<String, Statistics> entry : statsPerQueryName.entrySet()) {
        try (PrintStream ps = new PrintStream(outputDirectory + entry.getKey() + resultsFileName)) {
          rw.writeResults(entry.getValue(), ps);
        } catch (FileNotFoundException e) {
          e.printStackTrace();
        }
      }
      conn.close();
    } catch (SQLException ex) {
      ex.printStackTrace();
    }
  }

  private static Options buildOptions() {
    Options options = new Options();
    options.addOption("dm", true, "DataManger is executed when set");
    options.addOption("g", true, "Generator is executed when set");
    // currently ex is not needed to execute
    options.addOption("c", true, "Connection is established when set");
    options.addOption("d", false, "Drop tables. Only works if c is set.");
    options.addOption("a", true, "Launches anonymization process");
    return options;
  }

  public static String getSourcePath() {
    return sourcePath;
  }

  public static class QueryBool {
    public GenericQuery query;
    public boolean time;

    QueryBool(GenericQuery q, boolean b) {
      this.query = q;
      this.time = b;
    }
  }
}
