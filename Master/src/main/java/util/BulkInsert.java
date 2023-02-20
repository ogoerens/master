package util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BulkInsert extends GenericQuery {

  private String file;
  private String tbl;
  private String fieldterminator;
  private String rowterminator;
  public static String defaultFieldterminator = "' '";
  public static String defaultRowterminator = "'0x0A'";

  public BulkInsert(String file, String tbl) {
    this.file = file;
    this.tbl = tbl;
    this.fieldterminator = BulkInsert.defaultFieldterminator;
    this.rowterminator = BulkInsert.defaultRowterminator;
    qid = -1;
  }

  public BulkInsert(String file, String tbl, String fieldterminator) {
    this.file = file;
    this.tbl = tbl;
    this.fieldterminator = "'" + fieldterminator + "'";
    qid = -1;
  }

  public BulkInsert(String file, String tbl, String fieldterminator, String rowterminator) {
    this.file = file;
    this.tbl = tbl;
    this.fieldterminator = "'" + fieldterminator + "'";
    this.rowterminator = "'" + rowterminator + "'";
    qid = -1;
  }

  public void createSQL() {
    query_stmt =
        String.format(
            "BULK INSERT %s FROM %s WITH ( " + "fieldterminator= %s, " + "rowterminator= %s )",
            tbl, file, fieldterminator, rowterminator);
  }

  protected PreparedStatement getStatement(Connection conn) throws SQLException {
    if (query_stmt == null) {
      createSQL();
    }
    PreparedStatement stmt = conn.prepareStatement(query_stmt);
    return stmt;
  }
}
