package util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.random.*;

public class BulkInsert extends GenericQuery{

    private String file;
    private String tbl;
    private String defaultFieldterminator= "' '";
    private String defaultRowterminator= "'0x0A' ";
    public String query_stmt=null;
    public String qid="qBI";

    public BulkInsert(String file, String tbl){
        this.file=file;
        this.tbl=tbl;
    }
    public BulkInsert(String file, String tbl, String fieldterminator, String rowterminator){
        this.file=file;
        this.tbl=tbl;
        this.defaultFieldterminator=fieldterminator;
        this.defaultRowterminator=rowterminator;
    }

    public void createSQL(){
        query_stmt=String.format("BULK INSERT %s FROM %s WITH ( "
                + "fieldterminator= %s, "
                + "rowterminator= %s )", tbl, file, defaultFieldterminator, defaultRowterminator);

    }


    protected PreparedStatement getStatement(Connection conn) throws SQLException {
        if (query_stmt==null){
            createSQL();
        }
        PreparedStatement stmt = conn.prepareStatement(query_stmt);
        return stmt;
    }
}
