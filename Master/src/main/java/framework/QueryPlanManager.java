package framework;

import util.Utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;

public class QueryPlanManager {
    private Connection conn;
    private HashMap<Integer, Boolean> queryPlanStored;
    public QueryPlanManager(Connection conn){
        this.queryPlanStored = new HashMap<>();
        this.conn=conn;
    }

    public void storeQP(int qid, String queryIdentifier, String database){
        if (queryPlanStored.get(qid)!=null){
            return;
        }
        String str = "'%"+queryIdentifier+"%'";
        String sqlStmt ="SELECT Top(1) query_plan, dest.text"+
                " FROM sys.dm_exec_query_stats AS deqs " +
                " CROSS APPLY sys.dm_exec_query_plan(deqs.plan_handle) AS qp" +
                " CROSS APPLY sys.dm_exec_sql_text(deqs.sql_handle) AS dest" +
                " WHERE qp.dbid = DB_ID('"+database+"')" +
                " ORDER BY deqs.last_execution_time DESC";

        //System.out.println(sqlStmt);

        try{
            PreparedStatement stmt = conn.prepareStatement(sqlStmt);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()){
                String qp =rs.getString(1);
                String sqlText = rs.getString(2);
                //System.out.println(sqlText);
                String filenameQP = "qid"+qid+"QueryPlan";
                String filenameSQL = "qid"+qid+"SQLText";
                Utils.StrToFile(qp,filenameQP+".sqlplan");
                Utils.StrToFile(sqlText,filenameSQL+".sql");
            }
        }catch (java.sql.SQLException e){
            System.out.println("QueryPlanManager: " +e);


        }
        queryPlanStored.put(qid, true);
    }
}
