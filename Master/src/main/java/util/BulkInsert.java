package util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class BulkInsert extends GenericQuery {

  private String file;
  private String tbl;
  private String defaultFieldterminator = "' '";
  private String defaultRowterminator = "'0x0A' ";

  public BulkInsert(String file, String tbl) {
    this.file = file;
    this.tbl = tbl;
    qid = -1;
  }

  public BulkInsert(String file, String tbl, String fieldterminator, String rowterminator) {
    this.file = file;
    this.tbl = tbl;
    this.defaultFieldterminator = fieldterminator;
    this.defaultRowterminator = rowterminator;
    qid = -1;
  }

  public void createSQL() {
    query_stmt =
        String.format(
            "BULK INSERT %s FROM %s WITH ( " + "fieldterminator= %s, " + "rowterminator= %s )",
            tbl, file, defaultFieldterminator, defaultRowterminator);
  }

  protected PreparedStatement getStatement(Connection conn) throws SQLException {
    if (query_stmt == null) {
      createSQL();
    }
    PreparedStatement stmt = conn.prepareStatement(query_stmt);
    return stmt;
  }
}
