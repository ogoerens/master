package framework;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import util.Utils;

public class QueryPlanManager {
  private Connection conn;
  private HashMap<String, Integer> cardinalities;
  private HashMap<String, Boolean> queryPlanStored;
  private final String queryPlanFolder = Driver.getSourcePath() + "/QueryPlans";
  private final String queryStmtFolder = Driver.getSourcePath() + "/QueryStmts";

  public QueryPlanManager(Connection conn) {
    this.queryPlanStored = new HashMap<>();
    this.conn = conn;
    this.cardinalities = new HashMap<>();
  }

  /**
   * Retrieves and stores the query plan and the query text from the last query that was executed on
   * a specified database by analyzing the stats of this database. It does also retrieve the number
   * of rows returned from this query. However, currently it does not store this number.
   *
   * @param qid
   * @param queryIdentifier currently not used.
   * @param database The database for which we want to return the query plan of the last executed
   *     query.
   */
  public void storeQP (String qid, String queryIdentifier, String database) {
    //Create Folders which will store the QueryPLan and QueryStatement files.
    try {
      Files.createDirectories(Paths.get(queryPlanFolder));
      Files.createDirectories(Paths.get(queryStmtFolder));
    } catch(IOException e){
      System.err.println("QueryPlanManager could not create the QueryPlan and QueryStatement folders!");
      e.printStackTrace();
    }


    if (queryPlanStored.get(qid) != null) {
      return;
    }
    String sqlStmt =
        "SELECT Top(1) query_plan, dest.text, deqs.last_rows,*"
            + " FROM sys.dm_exec_query_stats AS deqs "
            + " CROSS APPLY sys.dm_exec_query_plan(deqs.plan_handle) AS qp"
            + " CROSS APPLY sys.dm_exec_sql_text(deqs.sql_handle) AS dest"
            + " WHERE qp.dbid = DB_ID('"
            + database
            + "')"
            + " ORDER BY deqs.last_execution_time DESC";

    try {
      PreparedStatement stmt = conn.prepareStatement(sqlStmt);
      ResultSet rs = stmt.executeQuery();
      while (rs.next()) {
        String qp = rs.getString(1);
        String sqlText = rs.getString(2);
        int total_rows = rs.getInt(3);

        String filenameQP = "qid" + qid + "QueryPlan";
        String filenameSQL = "qid" + qid + "SQLText";
        Utils.strToFile(qp, queryPlanFolder + "/" + filenameQP + ".sqlplan");
        Utils.strToFile(sqlText, queryStmtFolder + "/" + filenameSQL + ".sql");
        // Utils.StrToFile(Integer.toString(total_rows),directoryCardinality + "/"
        // +total_rows+".txt");
        cardinalities.put(qid, total_rows);
      }
    } catch (java.sql.SQLException e) {
      System.out.println("QueryPlanManager: " + e);
    }
    queryPlanStored.put(qid, true);
  }
}
