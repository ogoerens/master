package framework;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.sql.Connection;
import java.sql.SQLException;

import experimental.stuff;
import microbench.Queries;
import microbench.Query;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.tree.xpath.XPathExpressionEngine;
import org.apache.commons.configuration2.XMLConfiguration;

import util.ResultWriter;
import util.GenericQuery;

public class Driver {
  public static void main(String[] args) throws Exception {

    CommandLineParser parser = new DefaultParser();
    Options options = buildOptions();
    CommandLine argsLine = parser.parse(options, args);

    Random rand = new Random();

    // Check if Generator is used. If so, create generator configuration and execute generator. File
    // should be: genconfig.xml.

    if (argsLine.hasOption("g")) {
      String s = argsLine.getOptionValue("g");
      XMLConfiguration genConfiguration = buildXMLConfiguration(argsLine.getOptionValue("g"));
      Generator g = new Generator(rand);
      g.generate(genConfiguration);
    }

    // Create DB configuration  from configuration file.
    if (!argsLine.hasOption("c")) {
      System.out.println(
          "No Connection was established as argument -c was not set. "
              + "The framework will not be able perform any further operations");
      System.exit(0);
    }

    String dbConfigFile = argsLine.getOptionValue("c");
    XMLConfiguration dbConfiguration = buildXMLConfiguration(dbConfigFile);
    BenchConfiguration config = new BenchConfiguration(dbConfiguration);
    config.init();

    try {
      // Connect to database
      Connection conn = config.makeConnection();
      if (conn != null) {
        System.out.println("Connected.");
      }

      // Check if DataManager is used. If so, create DM configuration and execute DM. File should
      // be: manageconfig.xml.
      // DataManager is used to create/update the tables with generated data from files.

      if (argsLine.hasOption("dm")) {
        XMLConfiguration manConfiguration = buildXMLConfiguration(argsLine.getOptionValue("dm"));
        DataManager dm = new DataManager(conn);
        dm.manage(manConfiguration);
      }

      // Create queries from QueryString and add to transaction queue.
      int numberOfQueryExecutions = 10;
      ArrayList<Integer> qIDtoqName = new ArrayList<>();
      ArrayList<String> qIDtoqNameS = new ArrayList<>();
      ArrayList<String> qString = new ArrayList<>(Queries.count);
      qString.add(Queries.q0);
      qIDtoqName.add(-1);
      qIDtoqNameS.add("none");
      /*
        for (int i = 0; i < numberOfQueryExecutions; i++) {
          int j = 0;
          for (String query : Queries.queryList) {
            qString.add(query);
            qIDtoqName.add(j);
            qIDtoqNameS.add(Queries.queryListNames[j]);
            j++;
          }
        }
      */

      int j = 0;
      for (String query : Queries.queryList) {
        for (int i = 0; i < numberOfQueryExecutions; i++) {
          qString.add(query);
          qIDtoqName.add(j);
          qIDtoqNameS.add(Queries.queryListNames[j]);
        }
        j++;
      }

      ArrayList<Query> qList = Query.QueryGenerator.generateQueries(qString);

      // Could also define each Query as its own class.
      // Q0 q0 = new Q0();

      ArrayList<GenericQuery> transactionqueue = new ArrayList<>(qList);

      // Execute the Queries.
      int numberWorkers = 1;
      Worker w = new Worker(conn, transactionqueue, rand, numberWorkers);
      w.work(config.getDatabase());

      // Store cardinalities in a file.

      String directoryCardinality = "QueryCardinality";
      Path p = Paths.get(directoryCardinality + "/" + "total_rows.txt");
      Files.deleteIfExists(p);
      HashMap<String, Integer> qidToCardinality = new HashMap<>();
      HashSet<String> done = new HashSet<>();
      try (FileWriter cardinalityFileWriter =
          new FileWriter(directoryCardinality + "/" + "total_rows.txt", true)) {
        for (Map.Entry<Integer, Integer> entry : w.getCardinalities().entrySet()) {
          String x = qIDtoqNameS.get(entry.getKey());
          if (!done.contains(x)) {
            done.add(x);
            qidToCardinality.put(x, entry.getValue());
            cardinalityFileWriter.write(x + ": " + (entry.getValue()) + "\n");
          }
        }
        cardinalityFileWriter.flush();
      } catch (Exception e) {
        e.printStackTrace();
      }

      // Stats per qids individual.
      LinkedHashMap<String, LatencyRecord> latencyRecPerQid =
          w.getLatencyRecord().attachToQuery(qIDtoqNameS);
      LinkedHashMap<String, Statistics> statsPerQid = new LinkedHashMap<>();
      for (Map.Entry<String, LatencyRecord> entry : latencyRecPerQid.entrySet()) {
        statsPerQid.put(
            entry.getKey(), Statistics.computeStatistics(entry.getValue().getLatenciesAsArray()));
      }

      String resultOverviewFile = "overview.csv";
      try (PrintStream ps = new PrintStream(resultOverviewFile)) {
        ps.println(
            "Queries,returned rows,time(microseconds),75thPercentile(us), 90thPercentile(us)");
        for (Map.Entry<String, Statistics> entry : statsPerQid.entrySet()) {
          System.out.println(entry.getKey() + " : " + entry.getValue().getAverage());
          ps.println(
              entry.getKey()
                  + ","
                  + qidToCardinality.get(entry.getKey())
                  + ","
                  + entry.getValue().getAverage()
                  + ","
                  + entry.getValue().get75thPercentile()
                  + ","
                  + entry.getValue().get90thPercentile());
        }
      }

      // Stats for all qs together.
      Statistics stats = Statistics.computeStatistics(w.getLatencyRecord().getLatenciesAsArray());
      statsPerQid.put("-1", stats);
      System.out.println(stats.getAverage());

      // Close database connection.
      conn.close();

      // Write results.
      ResultWriter rw = new ResultWriter();
      String outputDirectory = "results";
      String resultsFileName = "results.csv";
      for (Map.Entry<String, Statistics> entry : statsPerQid.entrySet()) {
        if (!entry.getKey().equals("-1")) {
          try (PrintStream ps =
              new PrintStream(outputDirectory + "/QID" + entry.getKey() + resultsFileName)) {
            rw.writeResults(entry.getValue(), ps);
          } catch (FileNotFoundException e) {
            e.printStackTrace();
          }
        } else {
          try (PrintStream ps = new PrintStream(outputDirectory + "/" + resultsFileName)) {
            rw.writeResults(entry.getValue(), ps);
          } catch (FileNotFoundException e) {
            e.printStackTrace();
          }
        }
      }
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
    return options;
  }

  public static XMLConfiguration buildXMLConfiguration(String filename) {

    XMLConfiguration conf = new XMLConfiguration();
    Parameters params = new Parameters();

    FileBasedConfigurationBuilder<XMLConfiguration> builder =
        new FileBasedConfigurationBuilder<>(XMLConfiguration.class)
            .configure(
                params
                    .xml()
                    .setFileName(filename)
                    .setListDelimiterHandler(new DefaultListDelimiterHandler(','))
                    .setExpressionEngine(new XPathExpressionEngine()));

    try {
      conf = builder.getConfiguration();
    } catch (Exception e) {
      System.out.println("Configuration problem: " + e);
    }
    return conf;
  }
}
