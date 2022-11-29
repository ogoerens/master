package microbench;

import util.GenericQuery;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

public class Q0 extends GenericQuery {
    private String tbl;
    private ArrayList<String> column;
    private String criteria;

    private boolean adapted=false;

    public Q0(){
        this.query_stmt = null;
        this.qid=999;
    }
    public Q0(boolean adapted) {
        this.adapted=adapted;
        this.query_stmt = null;
        this.qid=999;
    }
        public void createSQL(){
        if (adapted){
            query_stmt= Queries.q1a;
        }else{
            query_stmt= Queries.q0;
        }
    }

    protected PreparedStatement getStatement(Connection conn) throws SQLException {
        if (query_stmt==null){
            createSQL();
        }
        //System.out.println(query_stmt);
        PreparedStatement stmt = conn.prepareStatement(query_stmt);
        return stmt;
    }
}
