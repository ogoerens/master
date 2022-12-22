package microbench;

import util.GenericQuery;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

public class Query extends GenericQuery {

    private static int counter =0;
    public static int dropQID =-2;

    public Query(String stmt){
        this.qid = counter++;
        this.query_stmt = stmt;
    }

    public Query(String stmt, int qid){
        this.qid = qid;
        this.query_stmt = stmt;
    }
    public int getqID(){
        return qid;
    }
    public String getQuery_stmt(){
        return query_stmt;
    }

    protected PreparedStatement getStatement(Connection conn) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(query_stmt);
        return stmt;
    }
    public static class  QueryGenerator {
        public static ArrayList<Query> generateQueries(ArrayList<String> queries){
            ArrayList<Query> generatedQueries = new ArrayList<>();
            for (String sqlStmt: queries){
                Query q = new Query(sqlStmt);
                generatedQueries.add(q);
            }
            return generatedQueries;
        }

        public static ArrayList<Query> generateDropQueries(String[] objectNames, String objectType){
            ArrayList<Query> generatedQueries = new ArrayList<>();
            for (String object:objectNames){
                String sqlStmt = String.format("Drop %s %s",objectType, object);
                Query q = new Query(sqlStmt,Query.dropQID);
                generatedQueries.add(q);
            }
            return generatedQueries;
        }


    }

}
