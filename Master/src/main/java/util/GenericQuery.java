package util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

  public void update(Connection conn) throws SQLException {

    try (PreparedStatement stmt = getStatement(conn)) {
      int r = stmt.executeUpdate();
      // System.out.println(r);
    }
  }
}
