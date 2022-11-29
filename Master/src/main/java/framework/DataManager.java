package framework;

import org.apache.commons.configuration2.XMLConfiguration;
import util.BulkInsert;

import java.sql.Connection;
import java.sql.Statement;

public class DataManager {
    private Connection conn;

    DataManager(Connection conn){
        this.conn = conn;
    }

    /**
     * Executes the operations as declared in the configuration.
     * @param conf
     */
    public void manage(XMLConfiguration conf){
        int amount = conf.getInt("amount");
        for (int i = 1; i<= amount; i++){
            String str = "man" + i + "/";
            String directory = System.getProperty("user.dir");
            String file = "'"+ directory+ "/" + conf.getString(str + "fileName") + "'";
            String tbl= conf.getString(str+ "table");
            String newTbl= conf.getString(str+ "newTable");
            String pk = conf.getString(str+ "primaryKey");
            String[] colTypes = conf.getStringArray(str+"types");
            String op =conf.getString(str + "operation");
            switch (op){
                case "updateTable":
                    updateTable(tbl, newTbl, pk, colTypes, file);
                    break;
                case "updateColumn":
                    String column = conf.getString(str+ "column");
                    updateColumn(tbl, newTbl, pk, column, colTypes[0], colTypes[1], file);
                    break;
                default: System.err.println("Non-matching operation in DataManager configuration file:" +op);
            }
        }
    }

    public void updateTable(String tbl,  String primaryKey, String[] columnTypes, String dataFile){
        String newTbl = "new"+tbl;
        updateTable(tbl, newTbl, primaryKey,columnTypes,dataFile);
    }

    /**
     * Creates a new Table containing the columns from the datafile. The column names for the "i"th added columns is: corr"i".
     * The new Table is called new"tbl".
     * @param tbl The table to which the columns are added.
     * @param primaryKey Indicates the Primary Key for tbl.
     * @param columnTypes Must indicate the type for each column in the datafile
     * @param dataFile Can contain arbitrary number of columns. However, the first column must be FK column for tbl.
     */
    public void updateTable(String tbl, String newTbl, String primaryKey, String[] columnTypes, String dataFile){
        String str = "(";
        int numberOfColumns =columnTypes.length;
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
            //System.out.println(dataFile);
            BulkInsert qNew = new BulkInsert(dataFile,"temp1");
            qNew.update(conn);
            str="";
            for (int i=1; i<numberOfColumns;i++ ){
                String corr="tbl2.corr";
                corr+= Integer.toString(i)+" ";
                str+= i==numberOfColumns-1? corr +" ":corr+", ";
            }
            String sqlStmt2= String.format("Select tbl1.*, %s into %s from %s as tbl1, temp1 as tbl2 where tbl1.%s = tbl2.corr0 ", str, newTbl, tbl ,primaryKey);
            stmt.executeUpdate(sqlStmt2);
            stmt.executeUpdate("DROP TABLE temp1");
        }catch (java.sql.SQLException e){
            e.printStackTrace();
        }
    }


    public void updateColumn(String tbl, String pk, String column, String keytype, String type, String dataFile){
        String newTbl = tbl + "_" + column + "updated";
        updateColumn(tbl, newTbl, pk, column,keytype,type,dataFile);
    }

    public void updateColumn(String tbl, String newTbl, String pk, String column, String keytype, String type, String dataFile){
        try{
           String[] typeArray ={keytype, type};
           updateTable(tbl, newTbl, pk , typeArray, dataFile);
           Statement stmt= conn.createStatement();
           String sqlStmt= String.format("ALTER TABLE %s DROP COLUMN %s", newTbl, column);
           stmt.executeUpdate(sqlStmt);
        }catch (java.sql.SQLException e){
            e.printStackTrace();
        }

    }
}
