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
    Query q = new Query(sqlStmt);
    ResultSet rs = q.runAndReturnResultSet(conn, new Random());
    while (rs.next()) {
      columnNames.add(rs.getString(1));
    }
    return columnNames;
  }

  public static ArrayList<String[]> getColumnNamesAndTypes(Connection conn, String tablename)
      throws SQLException {
    ArrayList<String[]> columnNamesAndTypes = new ArrayList<>();
    String sqlStmt =
        String.format(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name ='%s'",
            tablename);
    Query q = new Query(sqlStmt);
    ResultSet rs = q.runAndReturnResultSet(conn, new Random());
    while (rs.next()) {
      String[] columnNameAndType = {rs.getString(1), rs.getString(2)};
      columnNamesAndTypes.add(columnNameAndType);
    }
    return columnNamesAndTypes;
  }
}
