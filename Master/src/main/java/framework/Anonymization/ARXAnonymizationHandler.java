package framework.Anonymization;

import framework.DataManager;
import framework.DatabaseConfiguration;
import framework.Driver;
import org.apache.commons.configuration2.XMLConfiguration;
import org.deidentifier.arx.ARXAnonymizer;
import org.deidentifier.arx.ARXResult;
import util.Utils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class ARXAnonymizationHandler {
  private HierarchyManager hierarchyManager;
  private AnonymizationConfiguration anonConfig;
  private ARXDataHandler arxDataHandler;
  private AnonymizationStatistics anonStatistics;

  ARXAnonymizationHandler(
      HierarchyManager hierarchyManager, AnonymizationConfiguration anonconfig) {
    this.hierarchyManager = hierarchyManager;
    this.anonConfig = anonconfig;
  }

  public AnonymizationStatistics arxAnonymization(Connection conn, DatabaseConfiguration dbConfig)
      throws SQLException, Exception {
    // Build the hierarchies needed later on during the anonymization algorithm.
    this.hierarchyManager.buildHierarchies();
    HierarchyStore hierarchies = hierarchyManager.getHierarchyStore();
    // Retrieve column names and types from original table.
    this.arxDataHandler = new ARXDataHandler();
    ArrayList<String[]> columnNamesAndTypes =
        util.SQLServerUtils.getColumnNamesAndTypes(conn, anonConfig.getDataTableName());
    ArrayList<String> colNamesUppercase = new ArrayList<>();
    ArrayList<String> colTypes = new ArrayList<>();
    Set<String> numericColumns = new HashSet<>();
    for (String[] colNameAndType : columnNamesAndTypes) {
      colNamesUppercase.add(colNameAndType[0].toUpperCase());
      String type = (colNameAndType[1]);
      if (type.contains("char")) {
        colTypes.add(type + " " + Utils.surroundWithParentheses(colNameAndType[2]));
      } else {
        colTypes.add(type);
        numericColumns.add(colNameAndType[0]);
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
    ARXResult arxResult =
        anonymizer.anonymize(arxDataHandler.getArxdata(), anonConfig.getARXConfig());

    // Store the hierarchies for all hierarchies that are built using a HierarchyBuilder, i.e. by a
    // logic rather than an existing hierarchy file. Hierarchies are only built during the
    // anonymization process. Thus, they cannot be stored ahead of it.
    hierarchyManager.storeMaterializedHierarchies(arxDataHandler.getArxdata(), arxResult);
    hierarchyManager.storeMaterializedHierarchiesToFile();
    this.anonStatistics = new AnonymizationStatistics(arxResult, colNamesUppercase);
    Utils.strToFile(anonStatistics.printStats(), AnonymizationDriver.statsFile);

    // Anonym.printResult(arxResult, data);
    // TODO if not result possible with ldiversity. Check if can get ouput or output null.

    // Transform the arxResult data representation to a String no longer containing intervals.
    String outputString = cleanAndToString(arxResult, "|", System.lineSeparator(), numericColumns);
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

    return anonStatistics;
  }

  /**
   * Returns the anonymized dataset as String after cleaning the ARXResult such that it no longer
   * contains String intervals that cannot be converted to a numeric dataype. Intervals are
   * transformed by applying a function f([x,y[) = x.
   *
   * @param arxResult The variable containing the anonymized dataset.
   * @param elementDelimiter The delimiter that is inserted between elements when producing the
   *     String.
   * @param rowDelimiter THe delimiter that is sinserted between rows when the String is created.
   * @return
   */
  private String cleanAndToString(
      ARXResult arxResult,
      String elementDelimiter,
      String rowDelimiter,
      Set<String> numericColumns) {

    // Retrieve the column Names which have an interval builder.
    Set<String> intervalColumns = new HashSet<>();
    for (Map.Entry<String, String> hierarchyType : hierarchyManager.getHierarchyType().entrySet()) {
      if (hierarchyType.getValue().equals("interval")) {
        intervalColumns.add(hierarchyType.getKey());
      }
    }
    // Retrieve the column indexes in the data representation for the columns that actually contain
    // intervals. Columns that have not been generalized at all (generalizationLevel=0) or direct
    // identifiers that have been suppressed for example do not contain intervals even if they have
    // an interval builder logic.
    Set<Integer> intervalColumnsIndexes = new HashSet<>();
    for (String intervalCol : intervalColumns) {
      if (arxResult.getDataDefinition().getQuasiIdentifyingAttributes().contains(intervalCol)
          && this.anonStatistics.getGeneralizationLevels().get(intervalCol) != 0) {
        intervalColumnsIndexes.add(arxResult.getOutput().getColumnIndexOf(intervalCol));
      }
    }

    Set<Integer> sensitiveNumericColumnsIndexes = new HashSet<>();
    for (String numericColumn : numericColumns) {
      if (arxResult.getDataDefinition().getIdentifyingAttributes().contains(numericColumn)) {
        sensitiveNumericColumnsIndexes.add(arxResult.getOutput().getColumnIndexOf(numericColumn));
      }
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
        if (sensitiveNumericColumnsIndexes.contains(i)) {
          row[i] = "-1";
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

  /**
   * Loads a table from a DBMS into the ARX Data variable of the datahandler.
   *
   * @throws SQLException
   * @throws IOException
   */
  private void loadDataFromDBMS(DatabaseConfiguration dbConfig) throws SQLException, IOException {
    // The load function has its own connection to the DB server.
    arxDataHandler.loadJDBC(
        "jdbc:sqlserver://localhost:1433;encrypt=false;database=" + dbConfig.getDatabase() + ";",
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
}
