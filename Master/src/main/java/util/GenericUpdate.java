package util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.random.RandomGenerator;

public abstract class GenericUpdate {
  protected abstract PreparedStatement getStatement(
      Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException;

  public void update(Connection conn, RandomGenerator rand, double scaleFactor)
      throws SQLException {

    try (PreparedStatement stmt = getStatement(conn, rand, scaleFactor); ) {
      int r = stmt.executeUpdate();
    }
  }
}
