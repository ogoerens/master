package framework.Anonymization;

import framework.DataManager;
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

public class Transformer {
  // Query a whole table and insert transformed values in new table
  // Needs: connection to db, tablename, new tablename, transforming function.
  // Transforming function consists of: actual transformation for each column.
  Connection connection;
  UppercaseHashMap<String> collationInformation;

  public Transformer(Connection conn) {
    this.connection = conn;
  }

  public AnonymizationStatistics transform(
      String transformFunction, String tablename, String newTablename, String[] columns)
      throws SQLException {
    // Create new table.
    // Need to retrieve columnNames and Types.
    DataManager dataManager = new DataManager(this.connection);
    ArrayList<String[]> columnNamesAndTypes =
        util.SQLServerUtils.getColumnNamesAndTypes(this.connection, tablename);
    DBUtils.createTable(dataManager, columnNamesAndTypes, newTablename);

    //Gather collation information which may be needed in the transform function.
    this.collationInformation = gatherCollationInformation(connection, tablename);


    try {
      int batchSize = 50;
      String retrieveQueryString = "SELECT * FROM " + tablename;

      PreparedStatement stmt = this.connection.prepareStatement(retrieveQueryString);
      ResultSet rs = stmt.executeQuery();
      ResultSetMetaData rsMetaData = rs.getMetaData();
      int columnCount = rsMetaData.getColumnCount();

      this.connection.setAutoCommit(false);
      String columnPlaceHolders = "?,".repeat(columnCount);
      String storeQueryString =
          String.format(
              "INSERT INTO %s VALUES (%s)",
              newTablename, columnPlaceHolders.substring(0, columnPlaceHolders.length() - 1));
      PreparedStatement storeStatement = connection.prepareStatement(storeQueryString);

      int counter = 0;
      while (rs.next()) {
        ArrayList<String> row = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
          row.add(rs.getString(i));
          storeStatement.setString(i, rs.getString(i));
        }
        ArrayList<String> transformedRow =
            transformRow(transformFunction, row, columnNamesAndTypes, columns);
        for (int i = 0; i < columnCount; i++) {
          storeStatement.setString(i + 1, transformedRow.get(i));
        }
        storeStatement.addBatch();
        counter++;
        if (counter == batchSize) {
          int[] updateCounts = storeStatement.executeBatch();
          counter = 0;
        }
        // System.out.println(rs.getString(1));
        // do nothing
        // System.out.println("x");
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

    return new AnonymizationStatistics(this.collationInformation);
  }

  private ArrayList<String> transformRow(
      String transformFunction,
      ArrayList<String> row,
      ArrayList<String[]> columnNamesAndTypes,
      String[] hashingColumns) {
    switch (transformFunction) {
      case "SHA256":
        return transformRowSHA256(row, columnNamesAndTypes, hashingColumns);
      default:
        return transformRowSHA256(row, columnNamesAndTypes, hashingColumns);
    }
  }

  private ArrayList<String> transformRowSHA256(
      ArrayList<String> row, ArrayList<String[]> columnNamesAndTypes, String[] hashingColumns) {
    ArrayList<String> result = new ArrayList<>();
    for (int i = 0; i < columnNamesAndTypes.size(); i++) {
      String column = columnNamesAndTypes.get(i)[0];
      String value = row.get(i);
      //Attribute/columnn is not supposed to be transformed.
      if (!ArrayUtils.contains(hashingColumns, column)) {
        result.add(value);
        continue;
      }
      //Attribute/Column is supposed to be transformed.
      // We differ between Integers and Text, because they have different sizes. For text, the size can differ for each column.
      //TODO: Further differ between the different numeric types.
      if (columnNamesAndTypes.get(i)[1].contains("char")) {
        if (this.collationInformation.get(column).equals("insenstive")){
          value = value.toUpperCase();
        }
        String hash = DigestUtils.sha256Hex(value.trim());
        result.add(
            hash.substring(
                0, Math.min(Integer.parseInt(columnNamesAndTypes.get(i)[2]), hash.length())));
      } else {
        //Currently only correct for int, bigint and decimal. For smallint and tinyint the truncated hash value is still to big.
        //For bigint, and decimal the hash value does not need to be truncated as much.
        byte[] bytes = DigestUtils.sha256(row.get(i).trim());
        int num = ByteBuffer.wrap(Arrays.copyOfRange(bytes, 0, 4)).getInt();
        result.add(Integer.toString(num));
      }
    }

    AnonymizationStatistics anonymizationStatistics = new AnonymizationStatistics(this.collationInformation);
    return result;
  }

  public UppercaseHashMap<String> gatherCollationInformation(Connection conn, String tablename) throws SQLException{
    UppercaseHashMap<String> collationInformationMap = new UppercaseHashMap();
    String queryStmtString =
        "SELECT c.name ColumnName, collation_name FROM sys.columns c inner join sys.tables t on c.object_id = t.object_id WHERE t.name = "
            + Utils.surroundWith(tablename, "'");
    Query query = new Query(queryStmtString, "collationInformation", -1);
    ResultSet rs = query.runAndReturnResultSet(conn);
    while (rs.next()){
      String caseSensitivity="insensitive";
      String collation = rs.getString(2);
      if (collation==null){
        continue;
      }
      if (SQLServerUtils.isCaseSensitive(rs.getString(2))){
        caseSensitivity ="sensitive";
      }
      collationInformationMap.put(rs.getString(1),caseSensitivity);
    }
    return collationInformationMap;
  }
}
