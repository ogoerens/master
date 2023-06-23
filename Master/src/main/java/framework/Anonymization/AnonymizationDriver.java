package framework.Anonymization;

import framework.DatabaseConfiguration;
import framework.Driver;
import framework.QueryManager;
import microbench.Queries;
import org.apache.commons.configuration2.XMLConfiguration;
import org.deidentifier.arx.*;
import util.Utils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class AnonymizationDriver {
  String anonConfigFile;
  HierarchyManager hierarchyManager;
  AnonymizationConfiguration anonConfig;
  public static String statsFile = Driver.getSourcePath() + "/stats.txt";
  private static final String anonyimzedQueriesFile = "AnonymizedQueries.txt";
  private String dbConfigFile;
  private AnonymizationStatistics anonymizationStatistics;

  public AnonymizationDriver(String xmlConfig, String dbConfigFile) {
    this.anonConfigFile = xmlConfig;
    this.dbConfigFile = dbConfigFile;
    // Create the configuration for the anonymization process.
    this.anonConfig = new AnonymizationConfiguration(this.anonConfigFile);
  }

  /**
   * Anonymizes the data following the configuration specified by the Anonymization driver.
   *
   * @param queryManager
   * @throws SQLException
   * @throws Exception
   */
  public void anonymize(QueryManager queryManager) throws SQLException, Exception {
    XMLConfiguration xmlDbConfiguration = Utils.buildXMLConfiguration(dbConfigFile);
    DatabaseConfiguration dbConfiguration = new DatabaseConfiguration(xmlDbConfiguration);
    Connection conn = dbConfiguration.makeConnection();

    // Decide based on the anonymization strategy how the anonymization takes place.
    // Anonymize using hash.
    if (anonConfig.getAnonymizationStrategy().equalsIgnoreCase("hash")) {
      printTechnique("Hash");

      Transformer transformer = new Transformer(conn);
      String[] selectionCols = {"*"};
      this.anonymizationStatistics =
          transformer.transform(
              anonConfig.getHashingFunction(),
              anonConfig.getDataTableName(),
              anonConfig.getOutputTableName(),
              anonConfig.getHashingColumns(),
              selectionCols);
    } else {
      // Anonymize using Synthetic Data.
      if (anonConfig.getAnonymizationStrategy().equalsIgnoreCase("Synth")) {
        printTechnique("Synth");
        Synthesizer s = new Synthesizer(conn);
        s.synthesize(anonConfig, "src/main/resources/private-pgm/mechanisms/mst.py");

        String[] cols = {"*"};
        Transformer t = new Transformer(conn, new Random());
        t.transform(
            "SYNTHBACK",
            "RESULT",
            anonConfig.getOutputTableName(),
            cols,
            cols,
            util.SQLServerUtils.getColumnNamesAndTypes(conn, "customer"));

      } else {
        // Anonymize using the ARX library, i.e. using a syntactic privacy model.
        printTechnique("ARX");
        XMLConfiguration hierarchyConf = Utils.buildXMLConfiguration(anonConfig.getHierarchyFile());
        this.hierarchyManager = new HierarchyManager(hierarchyConf);
        ARXAnonymizationHandler arxAnonymizationHandler =
            new ARXAnonymizationHandler(hierarchyManager, anonConfig);
        this.anonymizationStatistics =
            arxAnonymizationHandler.arxAnonymization(conn, dbConfiguration);
      }
    }

    // Anonymization is finished, if Query anonymization is not asked for.
    if (!anonConfig.getQueryAnonimization()) {
      return;
    }

    // For synthetic data, queries do not change, except for the table.
    // First, the original queries are stored in the QueryManager, then they are anonymized and also
    // stored in the QueryManager.
    if (anonConfig.getAnonymizationStrategy().equalsIgnoreCase("Synth")) {
      for (String querySetName : anonConfig.getQuerysetNames()) {
        queryManager.addOriginalQueries(queryManager.returnQueryList(querySetName));
        for (microbench.Query query : queryManager.returnQueryList(querySetName)) {
          queryManager.addAnonymizedQuery(
              query.qName + "Anonymized",
              query
                  .query_stmt
                  .toUpperCase()
                  .replace(anonConfig.getDataTableName(), anonConfig.getOutputTableName()));
        }
      }
      return;
    }

    // For the methods using hashing or the ARX library, the query anonimzation process is the same.
    for (String querySetName : anonConfig.getQuerysetNames()) {
      queryManager.addOriginalQueries(queryManager.returnQueryList(querySetName));
    }
    ArrayList<String[]> columnNamesAndTypes =
        util.SQLServerUtils.getColumnNamesAndTypes(conn, anonConfig.getDataTableName());

    // Retrieve the generalization Levels for the anonymization and anonymize the queries.
    QueryAnonymizer queryAnonymizer =
        new QueryAnonymizer(
            this.anonymizationStatistics, hierarchyManager, anonConfig, columnNamesAndTypes);
    ArrayList<microbench.Query> anonQueries =
        queryAnonymizer.anonymize(queryManager.getOriginalQueryStore());
    queryManager.setAnonymizedQueryStore(anonQueries);

    // Print anonymized queries to a File.
    StringBuilder anonQueriesStringBuilder = new StringBuilder();
    for (microbench.Query anonymizedQuery : anonQueries) {
      anonQueriesStringBuilder.append("-".repeat(20));
      anonQueriesStringBuilder.append("QueryName: " + anonymizedQuery.qName + "\n");
      anonQueriesStringBuilder.append("Query: " + anonymizedQuery.query_stmt + "\n");
    }
    Utils.strToFile(anonQueriesStringBuilder.toString(), AnonymizationDriver.anonyimzedQueriesFile);

    conn.close();
  }

  private static void printTechnique(String technique) {
    System.out.println(technique);
  }
}
