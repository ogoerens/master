package util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.random.*;

public abstract class GenericQuery {
    public String query_stmt=null;
    public  int qid;

    protected abstract PreparedStatement getStatement(Connection conn) throws SQLException;


    public void run(Connection conn, RandomGenerator rand) throws SQLException {
        try (PreparedStatement stmt = getStatement(conn); ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                //do nothing
                //System.out.println("x");
            }
        }
    }

    public void update(Connection conn) throws SQLException {

        try
            (PreparedStatement stmt = getStatement(conn);){
             int r = stmt.executeUpdate();
             //System.out.println(r);
        }
    }
}
