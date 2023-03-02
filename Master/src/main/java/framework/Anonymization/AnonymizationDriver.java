package framework.Anonymization;

import framework.DataManager;
import framework.DatabaseConfiguration;
import framework.Driver;
import framework.QueryManager;
import microbench.Queries;
import org.apache.commons.configuration2.XMLConfiguration;
import org.deidentifier.arx.*;
import util.DBUtils;
import util.Utils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class AnonymizationDriver {
  String anonConfigFile;
  ARXDataHandler arxDataHandler;
  HierarchyManager hierarchyManager;
  AnonymizationConfiguration anonConfig;
  private static String statsFile = Driver.getSourcePath() +"/stats.txt";
  private static final String hierarchiesFile =
      "src/main/resources/hierarchies.xml";
  private static final String anonyimzedQueriesFile = "AnonymizedQueries.txt";
  private static final String dbConfigFile = "src/main/resources/benchconfigAnon.xml";
  private AnonymizationStatistics anonymizationStatistics;

  public AnonymizationDriver(String xmlConfig) {
    this.anonConfigFile = xmlConfig;
    // Create the configuration for the anonymization process.
    this.anonConfig = new AnonymizationConfiguration(this.anonConfigFile);
  }

  public void anonymize(QueryManager queryManager) throws SQLException, Exception {
    XMLConfiguration xmlDbConfiguration = Utils.buildXMLConfiguration(dbConfigFile);
    DatabaseConfiguration dbConfiguration = new DatabaseConfiguration(xmlDbConfiguration);
    Connection conn = dbConfiguration.makeConnection();


    //Decide based on the anonymization strategy how the anonyimization takes place.
    if (anonConfig.getAnonymizationStrategy().equalsIgnoreCase("hash")){
      //anonymizeUsingHash
      Transformer transformer = new Transformer(conn);
      this.anonymizationStatistics = transformer.transform(anonConfig.getHashingFunction(),anonConfig.getDataTableName(), anonConfig.getOutputTableName(), anonConfig.getHashingColumns());
    }else{
      //anonyimizeUSingArx
      this.anonymizationStatistics = arxAnonymization(conn, dbConfiguration);
    }

    // QueryAnonimization if set.
    if (!anonConfig.getQueryAnonimization()) {
      return;
    }

    // Add the queries specified in the configuration to the original Queryset in the queryMananger.
    for (String querySetName:anonConfig.getQuerysetNames()){
      queryManager.addOriginalQueries(Queries.returnQueryList(querySetName));
    }
    ArrayList<String[]> columnNamesAndTypes = util.SQLServerUtils.getColumnNamesAndTypes(
            conn, anonConfig.getDataTableName());

    // Retrieve the generalization Levels for the anonymization and anonymize the queries.
    QueryAnonymizer queryAnonymizer =
        new QueryAnonymizer(this.anonymizationStatistics, hierarchyManager, anonConfig, columnNamesAndTypes);
    ArrayList<microbench.Query> anonQueries =
        queryAnonymizer.anonymize(queryManager.getOriginalQueryStore());
    queryManager.setAnonymizedQueryStore(anonQueries);

    // Print anonymized queries to File.
    StringBuilder anonQueriesStringBuilder = new StringBuilder();
    for (microbench.Query anonymizedQuery : anonQueries) {
      anonQueriesStringBuilder.append("-".repeat(20));
      anonQueriesStringBuilder.append("QueryName: " + anonymizedQuery.qName + "\n");
      anonQueriesStringBuilder.append("Query: " + anonymizedQuery.query_stmt + "\n");
    }
    Utils.strToFile(anonQueriesStringBuilder.toString(),AnonymizationDriver.anonyimzedQueriesFile);

    conn.close();
  }

  /**
   * Loads a table from a DBMS into the data variable of the datahandler.
   * @throws SQLException
   * @throws IOException
   */
  private void loadDataFromDBMS(DatabaseConfiguration dbConfig) throws SQLException, IOException {
    // The load function has its own connection to the DB server.
    arxDataHandler.loadJDBC(
        "jdbc:sqlserver://localhost:1433;encrypt=false;database="
            + dbConfig.getDatabase()
            + ";",
        dbConfig.getUser(),
        dbConfig.getPassword(),
        anonConfig.getDataTableName());
  }

  private void loadDataFromFile() throws Exception {
    // TODO Not implemented currently.
    throw new Exception(
        "Loading a data from file has not yet been implemented. Please refer to loading data from a DBMS");
    // this.dataHandler.loadFile(anonConfig.getDataFileName(),AnonymizationConfiguration.charset,'|');
  }

  private String cleanseResultAndToString(
      ARXResult arxResult, String elementDelimiter, String rowDelimiter) {
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
      // Skip header containing the column names.
      if (header) {
        header = false;
        continue;
      }
      // Transform interval per element per row. Append elements in each row delimited by a specific
      // character.
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

  private AnonymizationStatistics arxAnonymization(Connection conn, DatabaseConfiguration dbConfig) throws SQLException, Exception{
    // Build the hierarchies needed later on during the anonymization algorithm.
    XMLConfiguration hierarchyConf = Utils.buildXMLConfiguration(hierarchiesFile);
    this.hierarchyManager = new HierarchyManager(hierarchyConf);
    this.hierarchyManager.buildHierarchies();
    HierarchyStore hierarchies = hierarchyManager.getHierarchyStore();
    this.arxDataHandler = new ARXDataHandler();
    ArrayList<String[]> columnNamesAndTypes =
            util.SQLServerUtils.getColumnNamesAndTypes(
                    conn, anonConfig.getDataTableName());
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
      this.loadDataFromDBMS(dbConfig);
    } else {
      this.loadDataFromFile();
    }

    // Enhance the data with the information contained in the configuration and add the hierarchies.
    arxDataHandler.applyConfigToData(anonConfig);
    arxDataHandler.addHierarchiesToData(hierarchies);

    // Run the dataset anonymization.
    ARXAnonymizer anonymizer = new ARXAnonymizer();
    ARXResult arxResult = anonymizer.anonymize(arxDataHandler.getArxdata(), anonConfig.getARXConfig());

    // Store the hierarchies for all hierarchies that are built using a HierarchyBuilder, i.e. by a
    // logic rather than an existing hierarchy file. Hierarchies are only built during the
    // anonymization process. Thus, they cannot be stored ahead of it.
    hierarchyManager.storeMaterializedHierarchies(arxDataHandler.getArxdata(), arxResult);
    hierarchyManager.storeMaterializedHierarchiesToFile();
    AnonymizationStatistics statistics = new AnonymizationStatistics(arxResult, colNamesUppercase);
    Utils.strToFile(statistics.printStats(),statsFile);

    // Anonym.printResult(arxResult, data);
    // TODO if not result possible with ldiversity. Check if can get ouput or output null.

    // Transform the arxResult data representation to a String no longer containing intervals.
    String outputString = cleanseResultAndToString(arxResult, "|", System.lineSeparator());
    Utils.strToFile(outputString, anonConfig.getOutputFileName());
    System.out.println("Wrote anonymized Data to " + anonConfig.getOutputFileName());

    // Insert the output of the anonymization in a new Table in the database.
    DataManager dataManger = new DataManager(conn);
    String[] cN = new String[colNamesUppercase.size()];
    String[] cT = new String[colTypes.size()];
    dataManger.newTable(
            anonConfig.getOutputTableName(),
            Driver.getSourcePath() + "/" + anonConfig.getOutputFileName(),
            colTypes.toArray(cT),
            colNamesUppercase.toArray(cN),
            "|",
            System.lineSeparator());

    return statistics;
  }
}
