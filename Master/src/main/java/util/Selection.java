package util;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.random.*;
import util.utils;

public class Selection extends GenericQuery{

    private String tbl;
    private String column;
    private String criteria;
    public String query_stmt=null;
    public String qid="qS";

    public Selection(String tbl, ArrayList<String> column, String criteria){
        this.tbl=tbl;
        this.column=utils.arrayListToSQLString(column);
        this.criteria=criteria;
    }

    public void createSQL(){
        query_stmt=String.format("SELECT %s FROM %s WHERE  "
                + " %s ", column, tbl,  criteria);
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
