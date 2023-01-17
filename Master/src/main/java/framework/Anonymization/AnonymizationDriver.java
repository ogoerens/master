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

  public void anonymize() throws SQLException, Exception {
    String anonConfigFile =
        "/home/olivier/Documents/MasterThesis/Master/src/main/resources/anonconfigCustomer.xml";
    String hierarchiesFile =
        "/home/olivier/Documents/MasterThesis/Master/src/main/resources/hierarchies.xml";
    String tableName = "customer";

    String dbConfigFile = "src/main/resources/benchconfigAnon.xml";

    XMLConfiguration dbConfiguration = Driver.buildXMLConfiguration(dbConfigFile);
    BenchConfiguration dbConfig = new BenchConfiguration(dbConfiguration);
    String database = dbConfig.getDatabase();
    dbConfig.init();
    Connection dbConnection = dbConfig.makeConnection();

    ArrayList<String> colNames = util.SQLServerUtils.getColumnNames(dbConnection, tableName);

    // Close the DB connection. All necessary information from the DB has been gathered.
    dbConnection.close();

    XMLConfiguration xmlAnonConfig = Driver.buildXMLConfiguration(anonConfigFile);
    AnonymizationConfiguration anonConfig = new AnonymizationConfiguration(xmlAnonConfig);
    anonConfig.createARXConfig();

    HierarchyManager hierarchyManager = new HierarchyManager();
    XMLConfiguration hierarchyConf = Driver.buildXMLConfiguration(hierarchiesFile);
    HierarchyStore hierarchies = hierarchyManager.buildHierarchies(hierarchyConf);



    DataHandler dataHandler = new DataHandler();

    dataHandler.loadJDBC(
        "jdbc:sqlserver://localhost:1433;encrypt=false;database=" + database + ";",
        "sa",
        ".+.QET21adg.+.",
        tableName);
    dataHandler.applyConfigToData(anonConfig);
    dataHandler.addHierarchiesToData( hierarchies);

    // Run the anonymization.
    ARXAnonymizer anonymizer = new ARXAnonymizer();
    ARXResult arxResult = anonymizer.anonymize(dataHandler.getData(), anonConfig.getARXConfig());

    // Store the hierarchies for all hierarchies that are built using a HierarchyBuilder, i.e. by a
    // Logic rather than an existing hierarchy file.

    for (String col : dataHandler.getData().getDefinition().getQuasiIdentifyingAttributes()) {
      if (hierarchies.getIndexForColumnName(col) == 1) {
        String fileName =
            StringUtil.createFileName(HierarchyManager.getHierarchyDirectory(), col, "csv");
        HierarchyManager.storeHierarchy(arxResult, col, fileName);
      }
    }

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
