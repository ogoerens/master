package util;

import microbench.Query;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;

public class SQLServerUtils {
  public static ArrayList<String> getColumnNames(Connection conn, String tablename)
      throws SQLException {
    ArrayList<String> columnNames = new ArrayList<>();
    String sqlStmt =
        String.format(
            "SELECT column_name FROM information_schema.columns WHERE table_name ='%s'", tablename);
    Query q = new Query(sqlStmt, Query.informationQID);
    ResultSet rs = q.runAndReturnResultSet(conn);
    while (rs.next()) {
      columnNames.add(rs.getString(1));
    }
    return columnNames;
  }

  /**
   * Returns the column names, types and character maximum length for a given table.
   * @param conn The connection to a database on a server.
   * @param tablename The name of the table for which the names and types are returned.
   * @return
   * @throws SQLException
   */
  public static ArrayList<String[]> getColumnNamesAndTypes(Connection conn, String tablename)
      throws SQLException {
    ArrayList<String[]> columnNamesAndTypes = new ArrayList<>();
    String sqlStmt =
        String.format(
            "SELECT column_name, data_type, CHARACTER_MAXIMUM_LENGTH FROM information_schema.columns WHERE table_name ='%s'",
            tablename);
    Query q = new Query(sqlStmt, Query.informationQID);
    ResultSet rs = q.runAndReturnResultSet(conn);
    while (rs.next()) {
      String[] columnNameAndType = {rs.getString(1).toUpperCase(), rs.getString(2), rs.getString(3)};
      columnNamesAndTypes.add(columnNameAndType);
    }
    return columnNamesAndTypes;
  }

  public static Boolean isCaseSensitive(String collation){
    return collation.contains("_CS_");
  }
}
