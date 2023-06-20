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
import org.apache.commons.cli.*;
import org.apache.commons.configuration2.XMLConfiguration;

import util.ResultWriter;
import util.GenericQuery;
import util.Utils;

public class Driver {
  public static final String sourcePath = System.getProperty("user.dir");
  private static final String outputDirectory = sourcePath + "/Results";
  private static final String CardinalityDirectory = sourcePath + "/QueryCardinality";
  private static final String latenciesFile = sourcePath + "/latencies.csv";
  private static final String latenciesFileAVG = sourcePath + "/latenciesAverage.csv";
  private static final int numberWorkers = 1;
  private static int numberOfQueryExecutions = 6;

  public static void main(String[] args) throws Exception {

    // Parse user options.
    CommandLineParser parser = new DefaultParser();
    Options options = buildOptions();
    CommandLine argsLine = parser.parse(options, args);

    Random rand = new Random();

    // Check if Generator is used. If so, create generator configuration and execute generator.
    if (argsLine.hasOption("g")) {
      XMLConfiguration genConfiguration = Utils.buildXMLConfiguration(argsLine.getOptionValue("g"));
      Generator g = new Generator(rand);
      g.generate(genConfiguration);
    }

    // Add user-provided queries to the Query Manager.
    QueryManager queryManager = new QueryManager();
    if (argsLine.hasOption("q")) {
      if (argsLine.hasOption("numExecutions")) {
        numberOfQueryExecutions = Integer.parseInt(argsLine.getOptionValue("numExecution"));
      }
      String queriesFile = argsLine.getOptionValue("q");
      queryManager.loadQueries(queriesFile, ";\n", "InputQueries_");
    }

    // Check if data should be anonymized.
    // Must be run after option "-q" as otherwise personalized queries are not yet stored in
    // queryManager.
    if (argsLine.hasOption("a")) {
      AnonymizationDriver ad =
          new AnonymizationDriver(argsLine.getOptionValue("a"), argsLine.getOptionValue("c"));
      // TODO: Detail what arguments anonymization driver takes.
      ad.anonymize(queryManager);
    }

    // Check if queries should be executed.
    if (argsLine.hasOption("e")) {
      if (argsLine.hasOption("numExecutions")) {
        numberOfQueryExecutions = Integer.parseInt(argsLine.getOptionValue("numExecution"));
      }
      String[] querySetNames = argsLine.getOptionValues("e");
      for (String querySetName : querySetNames) {
        queryManager.addQueriesForExecution(querySetName);
      }
    }


    // Check if option "-c" is set. If not, no connection to a database is established and the
    // program will exit.
    if (!argsLine.hasOption("c")) {
      System.out.println(
          "No Connection was established as argument -c was not set. "
              + "The framework will not be able perform any further operations");
      System.exit(0);
    }

    // Option "-c" is set. Create DB connection configuration from configuration file.
    String dbConfigFile = argsLine.getOptionValue("c");
    XMLConfiguration dbConfiguration = Utils.buildXMLConfiguration(dbConfigFile);
    DatabaseConfiguration config = new DatabaseConfiguration(dbConfiguration);

    // Connect to the database.
    try {
      Connection conn = config.makeConnection();
      if (conn != null) {
        System.out.println("Connected.");
      } else {
        throw new Exception("Connection to DB could not be established");
      }

      // Drop the tables that have been created during previous executions.
      // If value "anon" is passed as argument, the tables that are created during the anonymizatin
      // process are dropped.
      if (argsLine.hasOption("d")) {
        ArrayList<Query> qList = new ArrayList<>();
        if (argsLine.getOptionValue("d").equals("anon")) {
          String[] droptable = {
            "customerbefore",
            "anonymizedCustomer",
            "transformedData",
            "RemainingData",
            "SynthesizedData",
            "Result",
            "CorrectSynthesizedData",
            "maindata",
            "modifieddata"
          };
          qList.addAll(Query.QueryGenerator.generateDropQueries(droptable, "table"));
        } else {
          qList.addAll(Query.QueryGenerator.generateDropQueries(Queries.tables, "table"));
        }
        for (Query qqq : qList) {
          try {
            qqq.update(conn);
          } catch (SQLServerException e) {
            System.err.println(
                qqq.query_stmt
                    + " failed because the table does not exist or you do not have permission.");
          }
        }
        System.out.println("Drop was performed. Exiting");
        System.exit(0);
      }

      // Check if DataManager is used. If so, create DM configuration and execute DM.
      // DataManager is used to create/update the tables with generated data from files.
      if (argsLine.hasOption("dm")) {
        XMLConfiguration manConfiguration =
            Utils.buildXMLConfiguration(argsLine.getOptionValue("dm"));
        DataManager dm = new DataManager(conn);
        dm.manage(manConfiguration);
      }

      // Add queries to transactionqueue. Queries are only timed, if they are equal to the previous
      // query. This is done such that timed queries only run on a hot cache.
      ArrayList<QueryBool> transactionQueue = new ArrayList<>();
      String prevQuery = "";

      for (Query query : queryManager.getQueriesForExecution()) {
        for (int i = 0; i < numberOfQueryExecutions; i++) {
          if (prevQuery.equals(query.qName + query.query_stmt)) {
            transactionQueue.add(new QueryBool(query, true));
          } else {
            transactionQueue.add(new QueryBool(query, false));
            prevQuery = query.qName + query.query_stmt;
          }
        }
      }

      // Execute the Queries.
      Worker worker = new Worker(conn, transactionQueue, rand, numberWorkers);
      worker.work(config.getDatabase());

      // Store cardinalities in a file.
      Files.createDirectories(Paths.get(CardinalityDirectory));
      Path path = Paths.get(CardinalityDirectory + "/" + "queryCardinalitiesOverview.txt");
      Files.deleteIfExists(path);
      HashMap<String, Integer> qNameToCardinality = worker.getCardinalities();
      Utils.writeMapToFile(
          qNameToCardinality,
          CardinalityDirectory + "/" + "queryCardinalitiesOverview.txt",
          " : ",
          "\n");

      // For each query, group the execution per query Name. Compute for each of these groups the
      // statistics.
      Files.deleteIfExists(Paths.get(latenciesFile));
      StringBuilder stringBuilderLatencies = new StringBuilder();

      LinkedHashMap<String, LatencyRecord> latencyRecordPerQueryName =
          worker.getLatencyRecord().groupQueriesPerName();

      LinkedHashMap<String, LatencyRecord> latencyRecordPerDistribution =
          worker.getLatencyRecord().groupQueriesPerDistribution();

      LinkedHashMap<String, Statistics> statsPerQueryName = new LinkedHashMap<>();
      for (Map.Entry<String, LatencyRecord> entry : latencyRecordPerQueryName.entrySet()) {
        statsPerQueryName.put(
            entry.getKey(), Statistics.computeStatistics(entry.getValue().getLatenciesAsArray()));
        String latencies =
            Utils.join(
                Utils.convertIntArrayToStrArray(entry.getValue().getLatenciesAsArray()), ",");
        stringBuilderLatencies.append(entry.getKey());
        stringBuilderLatencies.append(",");
        stringBuilderLatencies.append(latencies);
        stringBuilderLatencies.append("\n");
      }
      Utils.strToFile(stringBuilderLatencies.toString(), latenciesFile);

      // "Clear" stringBuilder.
      stringBuilderLatencies.setLength(0);

      LinkedHashMap<String, Statistics> statsPerDistribution = new LinkedHashMap<>();
      for (Map.Entry<String, LatencyRecord> entry : latencyRecordPerDistribution.entrySet()) {
        statsPerDistribution.put(
            entry.getKey(), Statistics.computeStatistics(entry.getValue().getLatenciesAsArray()));
        String latencies =
            Utils.join(
                Utils.convertIntArrayToStrArray(entry.getValue().getLatenciesAsArray()), ",");
        stringBuilderLatencies.append(entry.getKey());
        stringBuilderLatencies.append(",");
        stringBuilderLatencies.append(latencies);
        stringBuilderLatencies.append("\n");
      }
      Utils.strToFile(stringBuilderLatencies.toString(), latenciesFileAVG);

      // Stats for all queries together.
      Statistics stats =
          Statistics.computeStatistics(worker.getLatencyRecord().getLatenciesAsArray());
      // statsPerQueryName.put("Overall", stats);

      // group by query number

      LinkedHashMap<String, ArrayList<Statistics>> statsPerQueryNumber = new LinkedHashMap<>();
      for (Map.Entry<String, Statistics> entry : statsPerQueryName.entrySet()) {
        String distribution = LatencyRecord.extractDistribution(entry.getKey());
        String querynumber =
            entry.getKey().substring(0, entry.getKey().indexOf("_")) + distribution;
        if (!statsPerQueryNumber.containsKey(querynumber)) {
          statsPerQueryNumber.put(querynumber, new ArrayList<>());
        }
        statsPerQueryNumber.get(querynumber).add(entry.getValue());
      }
      printAverageToFile(statsPerQueryNumber);

      ArrayList<String> statAttributes = new ArrayList<>();
      statAttributes.add("Average");
      statAttributes.add("Minimum");
      statAttributes.add("25thPercentile");
      statAttributes.add("Median");
      statAttributes.add("75thPercentile");
      statAttributes.add("90thPercentile");
      statAttributes.add("Maximum");
      // Store the statistics for each query group in the overview file.
      String resultOverviewFile = "overview.csv";
      StringBuilder stringBuilderStats = new StringBuilder();
      stringBuilderStats.append(
          "Queries, Returned rows, Average time(us), Minimum time, 25thPercentile, Median, 75thPercentile(us), 90thPercentile(us), Maximum \n");
      for (Map.Entry<String, Statistics> entry : statsPerQueryName.entrySet()) {
        System.out.println(entry.getKey() + " : " + entry.getValue().getAverage());
        stringBuilderStats.append(
            entry.getKey()
                + ","
                + qNameToCardinality.get(entry.getKey())
                + ","
                + entry.getValue().print(statAttributes)
                + System.lineSeparator());
      }
      Utils.strToFile(stringBuilderStats.toString(), resultOverviewFile);

      // Write a result file per Query Name.
      ResultWriter rw = new ResultWriter();
      Files.createDirectories(Paths.get(outputDirectory));
      String resultsFileName = "results.csv";
      for (Map.Entry<String, Statistics> entry : statsPerQueryName.entrySet()) {
        try (PrintStream ps =
            new PrintStream(outputDirectory + "/" + entry.getKey() + resultsFileName)) {
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

  /**
   * Creates the different options that user cans set when he runs the program
   * @return
   */
  private static Options buildOptions() {
    Options options = new Options();
    options.addOption("dm", true, "DataManger is executed when set");
    options.addOption("g", true, "Generator is executed when set");
    options.addOption("c", true, "Connection is established when set");
    options.addOption("q", true, "Executes queries provided in specified file");
    Option optionE = new Option("e", true, "Executes queries of specified benchmarks when set");
    optionE.setArgs(Option.UNLIMITED_VALUES);
    optionE.setValueSeparator(',');
    options.addOption(optionE);
    options.addOption("numExecutions", true, "Set number(int) of query executions. Default: 11");
    options.addOption("d", true, "Drop tables. Only works if c is set.");
    options.addOption("a", true, "Launches anonymization process");
    return options;
  }

  public static String getSourcePath() {
    return sourcePath;
  }

  public static void printAverageToFile(LinkedHashMap<String, ArrayList<Statistics>> stats) {
    // Store the statistics for each query group in the overview file.
    String resultOverviewFile = "averagesPerQueryNumber.csv";
    StringBuilder stringBuilderStats = new StringBuilder();
    stringBuilderStats.append("Queries,  Average time(us) \n");
    for (Map.Entry<String, ArrayList<Statistics>> entry : stats.entrySet()) {
      stringBuilderStats.append(entry.getKey());
      for (Statistics statistics : entry.getValue()) {
        stringBuilderStats.append("," + statistics.getAverage());
      }
      stringBuilderStats.append(System.lineSeparator());
    }
    Utils.strToFile(stringBuilderStats.toString(), resultOverviewFile);
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
