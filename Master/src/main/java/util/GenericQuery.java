package util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.random.*;

public abstract class GenericQuery {
  public String query_stmt = null;
  public int qid;

  protected abstract PreparedStatement getStatement(Connection conn) throws SQLException;

  public int run(Connection conn, RandomGenerator rand) throws SQLException {
    int countRows = 0;
    try (PreparedStatement stmt = getStatement(conn);
        ResultSet rs = stmt.executeQuery()) {
      while (rs.next()) {
        countRows++;
        // System.out.println(rs.getString(1));
        // do nothing
        // System.out.println("x");
      }
    }
    return countRows;
  }


  public void runAndStore(Connection connection, int numberOfArguments, String delimiter, String fileName) throws SQLException{
    StringBuilder result = new StringBuilder();
    try (PreparedStatement stmt = getStatement(connection);
         ResultSet rs = stmt.executeQuery()) {
      while (rs.next()) {
        for (int i=0; i< numberOfArguments; i++){
          result.append(rs.getString(1));
          result.append(delimiter);
        }
        // System.out.println(rs.getString(1));
        result.append("\n");
      }
    }
    Utils.StrToFile(result.toString(),fileName);
  }

  public ArrayList<String> runAndStoreFirstArgument(Connection connection) throws SQLException {
    ArrayList<String> result = new ArrayList<>();
    try (PreparedStatement stmt = getStatement(connection);
        ResultSet rs = stmt.executeQuery()) {
      while (rs.next()) {
        result.add(rs.getString(1));
      }
    }
    return result;
    }
    
  public ResultSet runAndReturnResultSet(Connection conn, RandomGenerator rand) throws SQLException {
    int countRows = 0;
    PreparedStatement stmt = getStatement(conn);
    ResultSet rs = stmt.executeQuery();
    return rs;
  }

  public void update(Connection conn) throws SQLException {

    try (PreparedStatement stmt = getStatement(conn)) {
      int r = stmt.executeUpdate();
      // System.out.println(r);
    }
  }
}
