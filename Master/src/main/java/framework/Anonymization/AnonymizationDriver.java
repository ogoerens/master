package framework.Anonymization;

import experimental.Anonym;
import framework.BenchConfiguration;
import framework.Driver;
import org.apache.commons.configuration2.XMLConfiguration;
import org.deidentifier.arx.*;
import org.deidentifier.arx.aggregates.HierarchyBuilder;
import util.StringUtil;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class AnonymizationDriver {
  String anonConfigFile =
      "/home/olivier/Documents/MasterThesis/Master/src/main/resources/anonconfigCustomer.xml";
  String hierarchiesFile =
      "/home/olivier/Documents/MasterThesis/Master/src/main/resources/hierarchies.xml";
  String tableName = "customer";
  String dbConfigFile = "src/main/resources/benchconfigAnon.xml";

  public void anonymize() throws SQLException, Exception {
    // Create the configuration for the anonymization process.
    AnonymizationConfiguration anonConfig = new AnonymizationConfiguration(anonConfigFile);
    anonConfig.createARXConfig();

    // Build the hierarchies needed later on during tha anonymization algorithm.
    XMLConfiguration hierarchyConf = Driver.buildXMLConfiguration(hierarchiesFile);
    HierarchyManager hierarchyManager = new HierarchyManager(hierarchyConf);
    hierarchyManager.buildHierarchies();
    HierarchyStore hierarchies = hierarchyManager.getHierarchyStore();

    // Load the data and all other information needed from the DB server.
    DataHandler dataHandler = new DataHandler();
    dataHandler.setDbConfiguration(dbConfigFile);
    dataHandler.connectToDB();

    ArrayList<String> colNames =
        util.SQLServerUtils.getColumnNames(dataHandler.getDbConnection(), tableName);

    // Close the DB connection. All necessary information from the DB has been gathered.
    dataHandler.getDbConnection().close();
    // The load function has its own connection to the DB server.
    dataHandler.loadJDBC(
        "jdbc:sqlserver://localhost:1433;encrypt=false;database="
            + dataHandler.getDbConfiguration().getDatabase()
            + ";",
        "sa",
        ".+.QET21adg.+.",
        tableName);
    // Enhance the data with the information contained in the configuration and add the hierarchies.
    dataHandler.applyConfigToData(anonConfig);
    dataHandler.addHierarchiesToData(hierarchies);

    // Run the anonymization.
    ARXAnonymizer anonymizer = new ARXAnonymizer();
    ARXResult arxResult = anonymizer.anonymize(dataHandler.getData(), anonConfig.getARXConfig());

    // Store the hierarchies for all hierarchies that are built using a HierarchyBuilder, i.e. by a
    // Logic rather than an existing hierarchy file. Hierarchies are only built during the
    // anonymization process. Thus they cannot be stored ahead of it.
    hierarchyManager.storeMaterializedHierarchies(dataHandler.getData(),arxResult);

    // Anonym.printResult(arxResult, data);
    System.out.println(" - Transformed data:");
    // TODO if not result possible with ldiversity. Check if can get ouput or output null.
    Iterator<String[]> transformed = arxResult.getOutput(false).iterator();
    while (transformed.hasNext()) {
      System.out.print("   ");
      System.out.println(Arrays.toString(transformed.next()));
    }

    AnonymizationStatistics anonStats = new AnonymizationStatistics(arxResult, colNames);
    anonStats.printStats();
  }
}
