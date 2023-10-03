package framework.Anonymization;

import framework.DataManager;
import framework.Generator;
import microbench.MicrobenchUtils;
import microbench.Query;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.ArrayUtils;

import util.DBUtils;
import util.SQLServerUtils;
import util.UppercaseHashMap;
import util.Utils;

import java.nio.ByteBuffer;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

public class Transformer {
  // Queries entire table and insert transformed values in new table
  // Needs: connection to db, tablename, new tablename, transforming function.
  // Transforming function consists of: actual transformation for each column.
  Connection connection;
  Generator g;
  UppercaseHashMap<String> collationInformation;

  public Transformer(Connection conn, Random r) {
    this.connection = conn;
    this.g = new Generator(r);
  }

  public Transformer(Connection conn) {
    this.connection = conn;
  }

  public AnonymizationStatistics transform(
      String transformFunction,
      String tablename,
      String newTablename,
      String[] columnsToTransform,
      String[] columnsToSelect)
      throws SQLException {
    ArrayList<String[]> columnNamesAndTypesOriginal =
        util.SQLServerUtils.getColumnNamesAndTypes(this.connection, tablename);
    return transform(
        transformFunction,
        tablename,
        newTablename,
        columnsToTransform,
        columnsToSelect,
        columnNamesAndTypesOriginal);
  }

  /**
   * Transforms the indicated columns using a specified transform function. A new table is created
   * to store the table with the new values.
   *
   * @param transformFunction Function used to transform the columns.
   * @param tablename Name of the old table with the not yet transformed columns.
   * @param newTablename Name of the new table that will contain the transformed columns.
   * @param columnsToTransform Names of the columns that should be transformed.
   * @param columnsToProject Names of the columns that should be kept in the new table.
   * @param newTableColumnTypes Type of the columns after applying the transformFunction.
   * @return
   * @throws SQLException
   */
  public AnonymizationStatistics transform(
      String transformFunction,
      String tablename,
      String newTablename,
      String[] columnsToTransform,
      String[] columnsToProject,
      ArrayList<String[]> newTableColumnTypes)
      throws SQLException {
    // Retrieve columnNames and Types from the original table.
    DataManager dataManager = new DataManager(this.connection);
    ArrayList<String[]> columnNamesAndTypesOriginal = newTableColumnTypes;
    HashMap<String, String[]> columnnameToType = new HashMap<>();
    for (String[] colNameAndType : columnNamesAndTypesOriginal) {
      String[] type = {colNameAndType[1], colNameAndType[2]};
      columnnameToType.put(colNameAndType[0], type);
    }

    // Reorder ColumnNamesANdTypes according to projection.
    String[][] columnNamesAndTypesArray;
    HashMap<String, Integer> columnIndex = new HashMap<>();
    for (int i = 0; i < columnsToProject.length; i++) {
      columnIndex.put(columnsToProject[i], i);
    }
    if (!(columnsToProject.length == 1 && columnsToProject[0].equals("*"))) {
      columnNamesAndTypesArray = new String[(columnsToProject.length)][3];
      for (int i = 0; i < columnNamesAndTypesOriginal.size(); i++) {
        String columnOriginal = columnNamesAndTypesOriginal.get(i)[0];
        if (columnIndex.containsKey(columnOriginal)) {
          columnNamesAndTypesArray[columnIndex.get(columnOriginal)] =
              columnNamesAndTypesOriginal.get(i);
        }
      }
    } else {
      columnNamesAndTypesArray = new String[(columnNamesAndTypesOriginal.size())][3];
      for (int i = 0; i < columnNamesAndTypesOriginal.size(); i++) {
        columnNamesAndTypesArray[i] = columnNamesAndTypesOriginal.get(i);
      }
    }
    // Gather collation information which may be needed in the transform function.
    this.collationInformation = gatherCollationInformation(connection, tablename);

    if (transformFunction.equalsIgnoreCase("project")){
      String projectionString = Utils.StrArrayToString(columnsToProject, ",", false);
      String retrieveQueryString = "SELECT " + projectionString + " INTO "+ newTablename  +" FROM " + tablename;

      PreparedStatement stmt = this.connection.prepareStatement(retrieveQueryString);
      stmt.executeUpdate();
    } else {

      try {
        int batchSize = 50;

        // Retrieve the old table from the DBMS.
        String projectionString = Utils.StrArrayToString(columnsToProject, ",", false);
        String retrieveQueryString = "SELECT " + projectionString + " FROM " + tablename;

        PreparedStatement stmt = this.connection.prepareStatement(retrieveQueryString);
        ResultSet rs = stmt.executeQuery();
        ResultSetMetaData rsMetaData = rs.getMetaData();

        // Create types based on metadata and input types.
        ArrayList<String[]> columnNamesAndTypesNewTable = new ArrayList<>();
        String s= rsMetaData.getColumnName(1);
        for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
          String[] nameandtype = {
            rsMetaData.getColumnName(i).toUpperCase(),
            columnnameToType.get(rsMetaData.getColumnName(i).toUpperCase())[0],
            columnnameToType.get(rsMetaData.getColumnName(i).toUpperCase())[1]
          };
          columnNamesAndTypesNewTable.add(nameandtype);
        }
        // Create the new table that will store the transformed dataset.
        DBUtils.createTable(dataManager, columnNamesAndTypesNewTable, newTablename);

        this.connection.setAutoCommit(false);
        // Create the SQL statement that inserted the transformed rows into the new table.
        int columnCount = rsMetaData.getColumnCount();
        String columnPlaceHolders = "?,".repeat(columnCount);
        String storeQueryString =
            String.format(
                "INSERT INTO %s VALUES (%s)",
                newTablename, columnPlaceHolders.substring(0, columnPlaceHolders.length() - 1));
        PreparedStatement storeStatement = connection.prepareStatement(storeQueryString);

        // Run the tansformation function and insertion statement for each row in the resultset. The
        // insertion is batched.
        int counter = 0;
        while (rs.next()) {
          ArrayList<String> row = new ArrayList<>();
          for (int i = 1; i <= columnCount; i++) {
            row.add(rs.getString(i));
          }
          ArrayList<String> transformedRow =
              transformRow(transformFunction, row, columnNamesAndTypesNewTable, columnsToTransform);
          for (int i = 0; i < columnCount; i++) {
            storeStatement.setString(i + 1, transformedRow.get(i));
          }
          storeStatement.addBatch();
          counter++;
          if (counter == batchSize) {
            int[] updateCounts = storeStatement.executeBatch();
            counter = 0;
          }
        }
        if (counter != 0) {
          storeStatement.executeBatch();
        }
        connection.commit();
      } catch (SQLException e) {
        e.printStackTrace();
      } finally {
        connection.setAutoCommit(true);
      }
    }
    return new AnonymizationStatistics(this.collationInformation);
  }

  /**
   * Transforms and stores the transformed table in a file. Otherwise same as transform.
   *
   * @param transformFunction
   * @param tablename
   * @param filename
   * @param columnsToTransform
   * @param columnsToSelect
   * @param delimiter
   * @param withHeader
   * @return
   * @throws SQLException
   */
  public AnonymizationStatistics transformAndStore(
      String transformFunction,
      String tablename,
      String filename,
      String[] columnsToTransform,
      String[] columnsToSelect,
      String delimiter,
      boolean withHeader)
      throws SQLException {

    ArrayList<String[]> columnNamesAndTypesOriginal =
        util.SQLServerUtils.getColumnNamesAndTypes(this.connection, tablename);

    // REORDER ColumnNamesANdTypes according to selection.
    String[][] columnNamesAndTypesArray = new String[(columnsToSelect.length)][3];
    HashMap<String, Integer> columnIndex = new HashMap<>();
    for (int i = 0; i < columnsToSelect.length; i++) {
      columnIndex.put(columnsToSelect[i], i);
    }
    if (!(columnsToSelect.length == 1 && columnsToSelect[0].equals("*"))) {
      for (int i = 0; i < columnNamesAndTypesOriginal.size(); i++) {
        String columnOriginal = columnNamesAndTypesOriginal.get(i)[0];
        if (columnIndex.containsKey(columnOriginal)) {
          columnNamesAndTypesArray[columnIndex.get(columnOriginal)] =
              columnNamesAndTypesOriginal.get(i);
        }
      }
    } else {
      for (int i = 0; i < columnNamesAndTypesOriginal.size(); i++) {
        columnNamesAndTypesArray[i] = columnNamesAndTypesOriginal.get(i);
      }
    }
    ArrayList<String[]> columnNamesAndTypes =
        new ArrayList<>(Arrays.asList(columnNamesAndTypesArray));

    // Gather collation information which may be needed in the transform function.
    this.collationInformation = gatherCollationInformation(connection, tablename);

    try {
      String selectionString = Utils.StrArrayToString(columnsToSelect, ",", false);
      String retrieveQueryString = "SELECT " + selectionString + " FROM " + tablename;

      PreparedStatement stmt = this.connection.prepareStatement(retrieveQueryString);
      ResultSet rs = stmt.executeQuery();
      ResultSetMetaData rsMetaData = rs.getMetaData();
      int columnCount = rsMetaData.getColumnCount();

      StringBuilder stringBuilderOutput = new StringBuilder();
      if (withHeader) {
        stringBuilderOutput.append(Utils.StrArrayToString(columnsToSelect, delimiter, false));
        stringBuilderOutput.append("\n");
      }
      while (rs.next()) {
        ArrayList<String> row = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
          row.add(rs.getString(i));
        }
        ArrayList<String> transformedRow =
            transformRow(transformFunction, row, columnNamesAndTypes, columnsToTransform);
        stringBuilderOutput.append(Utils.join(transformedRow, delimiter));
        stringBuilderOutput.append("\n");
      }
      Utils.strToFile(stringBuilderOutput.toString(), filename + ".csv");
    } catch (SQLException e) {
      e.printStackTrace();
    }

    return new AnonymizationStatistics(this.collationInformation);
  }

  /**
   * Transforms a single row with the indicated transformfunction.
   *
   * @param transformFunction
   * @param row
   * @param columnNamesAndTypes
   * @param hashingColumns
   * @return
   */
  private ArrayList<String> transformRow(
      String transformFunction,
      ArrayList<String> row,
      ArrayList<String[]> columnNamesAndTypes,
      String[] hashingColumns) {
    switch (transformFunction) {
      case "SYNTHBACK":
        return transformBackSynth(
            row,
            columnNamesAndTypes,
            hashingColumns,
            Utils.createMapping(MicrobenchUtils.mktsegmentValues));
      case "SYNTH":
        return transformRowSynth(
            row,
            columnNamesAndTypes,
            hashingColumns,
            Utils.createCategoricalMapping(MicrobenchUtils.mktsegmentValues));
      case "SHA256":
        return transformRowSHA256(row, columnNamesAndTypes, hashingColumns);
      default:
        return transformRowSHA256(row, columnNamesAndTypes, hashingColumns);
    }
  }

  private ArrayList<String> transformRowSynth(
      ArrayList<String> row,
      ArrayList<String[]> columnNamesAndTypes,
      String[] columnsToTransform,
      HashMap<String, Integer> mapping_mkt) {
    ArrayList<String> result = new ArrayList<>();
    for (int i = 0; i < columnNamesAndTypes.size(); i++) {
      String column = columnNamesAndTypes.get(i)[0];
      String value = row.get(i).strip();
      // Attribute/columnn is not supposed to be transformed.
      if (!ArrayUtils.contains(columnsToTransform, column)) {
        result.add(value);
        continue;
      }
      if (column.equals("C_ACCTBAL")) {
        value = value.substring(0, value.indexOf("."));
        int minimum = 1000;
        int numericValue = Integer.valueOf(value) + minimum;
        value = Integer.toString(numericValue);
      }
      /*if (column.equals("CORR2")) {
        int minimum = 100;
        int numericValue = Integer.valueOf(value) + minimum;
        value = Integer.toString(numericValue);
      }*/
      if (column.equals("C_MKTSEGMENT")) {
        value = Integer.toString(mapping_mkt.get(value));
      }
      if (column.equals("C_PHONE")) {
        String replaceStr = "-";
        value = value.replaceAll(replaceStr, "").substring(0, 5);
        int minimum = 10000;
        int numericValue = Integer.valueOf(value) - minimum;
        value = Integer.toString(numericValue);
      }
      result.add(value);
    }

    return result;
  }

  private ArrayList<String> transformBackSynth(
      ArrayList<String> row,
      ArrayList<String[]> columnNamesAndTypes,
      String[] columnsToTransform,
      HashMap<Integer, String> mapping_mkt) {
    ArrayList<String> result = new ArrayList<>();
    for (int i = 0; i < columnNamesAndTypes.size(); i++) {
      String column = columnNamesAndTypes.get(i)[0];
      String value = row.get(i).strip();
      // Attribute/columnn is not supposed to be transformed.

      if (column.equals("C_ACCTBAL")) {
        int[] decimals = this.g.generateUniform(1, 0, 100);
        int minimum = 1000;
        int numericValue = Integer.valueOf(value) - minimum;
        value = numericValue + "." + decimals[0];
      }
      /*if (column.equals("CORR2")) {
        int minimum = 100;
        int numericValue = Integer.valueOf(value) - minimum;
        value = Integer.toString(numericValue);
      }*/
      if (column.equals("C_MKTSEGMENT")) {
        value = mapping_mkt.get(Integer.valueOf(value));
      }
      if (column.equals("C_PHONE")) {
        String replaceStr = "-";
        int[] phone_ending = this.g.generateUniform(1, 0, 9999999);
        int missingZeroes = 7 - Integer.toString(phone_ending[0]).length();
        int minimum = 10000;
        int numericValue = Integer.valueOf(value) + minimum;
        value = Integer.toString(numericValue) + "0".repeat(missingZeroes) + phone_ending[0];
        value =
            value.substring(0, 2)
                + replaceStr
                + value.substring(2, 5)
                + replaceStr
                + value.substring(5, 8)
                + replaceStr
                + value.substring(8, 12);
      }
      result.add(value);
    }

    return result;
  }

  /**
   * Applies the hash function SHA256 to the columns.
   *
   * @param row
   * @param columnNamesAndTypes
   * @param hashingColumns
   * @return
   */
  private ArrayList<String> transformRowSHA256(
      ArrayList<String> row, ArrayList<String[]> columnNamesAndTypes, String[] hashingColumns) {
    ArrayList<String> result = new ArrayList<>();
    for (int i = 0; i < columnNamesAndTypes.size(); i++) {
      String column = columnNamesAndTypes.get(i)[0];
      String value = row.get(i);
      // Attribute/columnn is not supposed to be transformed.
      if (!ArrayUtils.contains(hashingColumns, column)) {
        result.add(value);
        continue;
      }
      // Attribute/Column is supposed to be transformed.
      // We differ between Integers and Text, because they have different sizes. For text, the size
      // can differ for each column.
      // TODO: Further differ between the different numeric types.
      if (columnNamesAndTypes.get(i)[1].contains("char")) {
        if (this.collationInformation.get(column).equals("insensitive")) {
          value = value.toUpperCase();
        }
        String hash = DigestUtils.sha256Hex(value.trim());
        result.add(
            hash.substring(
                0, Math.min(Integer.parseInt(columnNamesAndTypes.get(i)[2]), hash.length())));
      } else {
        // Currently only correct for int, bigint and decimal. For smallint and tinyint the
        // truncated hash value is still to big.
        // For bigint, and decimal the hash value does not need to be truncated as much.
        if (value.contains(".") || value.contains(",")) {}
        byte[] bytes = DigestUtils.sha256(value.trim());
        int num = ByteBuffer.wrap(Arrays.copyOfRange(bytes, 0, 4)).getInt();
        result.add(Integer.toString(num));
      }
    }

    return result;
  }


  /**
   * Gathers the collation information for a specific table in a database server.
   *
   * @param conn The SQL connection to a DBMS server.
   * @param tablename The tablename of the table for which the collation information is retrieved,
   * @return
   * @throws SQLException
   */
  public UppercaseHashMap<String> gatherCollationInformation(Connection conn, String tablename)
      throws SQLException {
    UppercaseHashMap<String> collationInformationMap = new UppercaseHashMap();
    String queryStmtString =
        "SELECT c.name ColumnName, collation_name FROM sys.columns c inner join sys.tables t on c.object_id = t.object_id WHERE t.name = "
            + Utils.surroundWith(tablename, "'");
    Query query = new Query(queryStmtString, "collationInformation", -1);
    ResultSet rs = query.runAndReturnResultSet(conn);
    while (rs.next()) {
      String caseSensitivity = "insensitive";
      String collation = rs.getString(2);
      if (collation == null) {
        continue;
      }
      if (SQLServerUtils.isCaseSensitive(rs.getString(2))) {
        caseSensitivity = "sensitive";
      }
      collationInformationMap.put(rs.getString(1), caseSensitivity);
    }
    return collationInformationMap;
  }
}
