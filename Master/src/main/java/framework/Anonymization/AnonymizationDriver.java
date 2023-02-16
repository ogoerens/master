package framework.Anonymization;

import framework.DataManager;
import framework.QueryManager;
import microbench.Queries;
import org.apache.commons.configuration2.XMLConfiguration;
import org.deidentifier.arx.*;
import util.Utils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class AnonymizationDriver {
  String anonConfigFile;
  DataHandler dataHandler;
  HierarchyManager hierarchyManager;
  AnonymizationConfiguration anonConfig;
  String hierarchiesFile =
      "/home/olivier/Documents/MasterThesis/Master/src/main/resources/hierarchies.xml";
  String dbConfigFile = "src/main/resources/benchconfigAnon.xml";

  public AnonymizationDriver(String xmlConfig) {
    this.anonConfigFile = xmlConfig;
    // Create the configuration for the anonymization process.
    this.anonConfig = new AnonymizationConfiguration(this.anonConfigFile);
  }

  public void anonymize(QueryManager queryManager) throws SQLException, Exception {

    // Build the hierarchies needed later on during the anonymization algorithm.
    XMLConfiguration hierarchyConf = Utils.buildXMLConfiguration(hierarchiesFile);
    this.hierarchyManager = new HierarchyManager(hierarchyConf);
    this.hierarchyManager.buildHierarchies();
    HierarchyStore hierarchies = hierarchyManager.getHierarchyStore();

    this.dataHandler = new DataHandler();
    this.dataHandler.setDbConfiguration(dbConfigFile);
    this.dataHandler.connectToDB();

    ArrayList<String[]> columnNamesAndTypes =
        util.SQLServerUtils.getColumnNamesAndTypes(
            dataHandler.getDbConnection(), anonConfig.getDataTableName());
    ArrayList<String> colNamesUppercase = new ArrayList<>();
    ArrayList<String> colTypes = new ArrayList<>();
    for (String[] colNameAndType : columnNamesAndTypes) {
      colNamesUppercase.add(colNameAndType[0].toUpperCase());
      String type = (colNameAndType[1]);
      if (type.contains("char")) {
        colTypes.add(type + " " + Utils.surroundWithParentheses(colNameAndType[2]));
      } else {
        colTypes.add(type);
      }
    }

    // Load the data and all other information needed from the DB server.
    if (anonConfig.getDataStorageMethod().equalsIgnoreCase("dbms")) {
      this.loadDataFromDBMS();
    } else {
      this.loadDataFromFile();
    }

    // Enhance the data with the information contained in the configuration and add the hierarchies.
    dataHandler.applyConfigToData(anonConfig);
    dataHandler.addHierarchiesToData(hierarchies);

    // Run the dataset anonymization.
    ARXAnonymizer anonymizer = new ARXAnonymizer();
    ARXResult arxResult = anonymizer.anonymize(dataHandler.getData(), anonConfig.getARXConfig());

    // Store the hierarchies for all hierarchies that are built using a HierarchyBuilder, i.e. by a
    // logic rather than an existing hierarchy file. Hierarchies are only built during the
    // anonymization process. Thus, they cannot be stored ahead of it.
    hierarchyManager.storeMaterializedHierarchies(dataHandler.getData(), arxResult);
    AnonymizationStatistics anonStats = new AnonymizationStatistics(arxResult, colNamesUppercase);
    anonStats.printStats();

    // Anonym.printResult(arxResult, data);
    // TODO if not result possible with ldiversity. Check if can get ouput or output null.

    // Transform the arxResult data representation to a String no longer containing intervals.
    String outputString = cleanseResultAndToString(arxResult,"|","\n");
    Utils.StrToFile(outputString, anonConfig.getOutputFileName());
    System.out.println("Wrote anonymized Data to " + anonConfig.getOutputFileName());

    // Insert the output in a new Table in the database.
    DataManager dataManger = new DataManager(dataHandler.getDbConnection());
    String[] cN = new String[colNamesUppercase.size()];
    String[] cT = new String[colTypes.size()];
    dataManger.newTable(
        anonConfig.getOutputTableName(),
        "'" + System.getProperty("user.dir") + anonConfig.getOutputFileName(),
        colTypes.toArray(cT),
        colNamesUppercase.toArray(cN),
        "|");
    dataHandler.getDbConnection().close();


    // QueryAnonimization.
    QueryAnonymizer queryAnonymizer =
        new QueryAnonymizer(anonStats.getGeneralizationLevels(), hierarchyManager);
    ArrayList<microbench.Query> anonQueries =
        queryAnonymizer.anonymize(queryManager.getOriginalQueries());
    queryManager.setAnonymizedQueryStore(anonQueries);

    for (microbench.Query anonymizedQuery : anonQueries) {
      System.out.println(
          "QueryName: "
              + anonymizedQuery.qName
              + "\n Query: "
              + anonymizedQuery.query_stmt
              + "\n");
    }
  }

  private void loadDataFromDBMS() throws SQLException, IOException {
    // The load function has its own connection to the DB server.
    dataHandler.loadJDBC(
        "jdbc:sqlserver://localhost:1433;encrypt=false;database="
            + dataHandler.getDbConfiguration().getDatabase()
            + ";",
        "sa",
        ".+.QET21adg.+.",
        anonConfig.getDataTableName());
  }

  private void loadDataFromFile() throws Exception {
    // TODO Not implemented currently.
    throw new Exception(
        "Loading a data from file has not yet been implemented. Please refer to loading data from a DBMS");
    // this.dataHandler.loadFile(anonConfig.getDataFileName(),AnonymizationConfiguration.charset,'|');
  }

  private String cleanseResultAndToString(ARXResult arxResult, String elementDelimiter, String rowDelimiter) {
    // Get Information for cleansing. Notably Transforming String intervals to integer intervals
    // ([x,y[ --> x).
    // Retrieve the column Names which contain intervals.
    Set<String> intervalColumns = new HashSet<>();
    for (Map.Entry<String, String> hierarchyType : hierarchyManager.getHierarchyType().entrySet()) {
      if (hierarchyType.getValue().equals("interval")) {
        intervalColumns.add(hierarchyType.getKey());
      }
    }
    // Retrieve the column indexes in the data representation for the columns that contain
    // intervals.
    Set<Integer> intervalColumnsIndexes = new HashSet<>();
    for (String intervalCol : intervalColumns) {
      intervalColumnsIndexes.add(arxResult.getOutput().getColumnIndexOf(intervalCol.toUpperCase()));
    }

    Iterator<String[]> outputIterator = arxResult.getOutput(false).iterator();
    StringBuilder stringBuilderTable = new StringBuilder();
    boolean header = true;
    while (outputIterator.hasNext()) {
      String[] row = outputIterator.next();
      StringBuilder stringBuilderRow = new StringBuilder();
      //Skip header containing the column names.
      if (header) {
        header = false;
        continue;
      }
      // Transform interval per element per row. Append elements in each row delimited by a specific character.
      for (int i = 0; i < row.length; i++) {
        if (intervalColumnsIndexes.contains(i)) {
          row[i] = (ARXUtils.removeInterval(row[i]));
        }
        if (i == row.length - 1) {
          stringBuilderRow.append(row[i]);
        } else {
          stringBuilderRow.append(row[i] + elementDelimiter);
        }
      }
      stringBuilderTable.append(stringBuilderRow.toString() + rowDelimiter);
    }
    return stringBuilderTable.toString();
  }


}
