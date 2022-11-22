package framework;

import util.BulkInsert;

import java.sql.Connection;
import java.sql.Statement;

public class DataManager {
    private Connection conn;

    DataManager(Connection conn){
        this.conn = conn;
    }

    public void updateTable(String tbl, String primaryKey, int numberOfColumns, String[] columnTypes, String dataFile){
        String str = "(";
        for (int i=0; i<numberOfColumns;i++ ){
            String corr="corr";
            corr += Integer.toString(i)+" "+ columnTypes[i];
            str += i==numberOfColumns-1? corr +" ":corr+", ";
        }
        str += ")";
        try{
            Statement stmt= conn.createStatement();
            String sqlStmt= "CREATE table temp1"+str;
            stmt.executeUpdate(sqlStmt);
            BulkInsert qNew = new BulkInsert(dataFile,"temp1");
            qNew.update(conn);

            str="";
            for (int i=1; i<numberOfColumns;i++ ){
                String corr="tbl2.corr";
                corr+= Integer.toString(i)+" ";
                str+= i==numberOfColumns-1? corr +" ":corr+", ";
            }

            String sqlStmt2= String.format("Select tbl1.*, %s into resultingCust from %s as tbl1, temp1 as tbl2 where tbl1.%s = tbl2.corr0 ", str, tbl ,primaryKey);
            stmt.executeUpdate(sqlStmt2);
            stmt.executeUpdate("DROP TABLE temp1");
        }catch (java.sql.SQLException e){
            System.out.println(e);
        }
    }


    public void updateColumn(String tbl, String pk, String column, String keytype, String type, String dataFile){
        try{
           Statement stmt= conn.createStatement();
           String sqlStmt= String.format("ALTER %s DROP COLUMN %s", tbl, column);
           String[] typeArray ={keytype, type};
           updateTable(tbl, pk, 2, typeArray, dataFile);

        }catch (java.sql.SQLException e){
            System.out.println(e);
        }

    }
}
