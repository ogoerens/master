package framework;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.beanutils.converters.SqlDateConverter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import util.BulkInsert;
import util.Utils;

public class DataManager {
  private Connection conn;

  DataManager(Connection conn) {
    this.conn = conn;
  }

  /**
   * Executes the operations as declared in the configuration. Checks how many SQL statement and
   * general Updates are declared in the configuration file. It then executes first the SQL update
   * statements, before continuing with the general updates. The general updates are divided in
   * column updates and table updates. Depending on the case, a different function is called that
   * takes care of the update.
   *
   * @param conf Configuration file containing information about tables that need to be updated or
   *     newly created.
   */
  public void manage(XMLConfiguration conf) {
    int amountSQL = conf.getInt("amountSQL");
    int amountFile = conf.getInt("amountFile");
    int amountIndex = conf.getInt("amountIndex");
    //Index for configurationAt method starts at 1!
    for (int i = 1; i <= amountSQL; i++) {
      HierarchicalConfiguration subConfig = conf.configurationAt("manSQL[" + i + "]");
      String sqlStmt = subConfig.getString("SQLStmt");
      update(sqlStmt);
    }

    for (int i = 1; i <= amountFile; i++) {
      HierarchicalConfiguration subConfig = conf.configurationAt("manFile[" + i + "]");
      String directory = System.getProperty("user.dir")+"/generated";
      String file = "'" + directory + "/" + subConfig.getString("fileName") + "'";
      String tbl = subConfig.getString("table");
      String newTbl = subConfig.getString("newTable");
      String pk = subConfig.getString("primaryKey");
      String[] colTypes = subConfig.getStringArray("columnTypes");
      String[] colNames = subConfig.getStringArray("columnNames");
      String op = subConfig.getString("operation");
      switch (op) {
        case "updateTable":
          updateTable(tbl, newTbl, pk, colTypes, colNames, file);
          break;
        case "updateColumn":
          String column = subConfig.getString("column");
          updateColumn(tbl, newTbl, pk, column, colTypes[0], colTypes[1], colNames, file);
          break;
        case "createIndexOnCopy":

        default:
          System.err.println("Non-matching operation in DataManager configuration file:" + op);
      }
    }
    for (int i=1; i<= amountIndex; i++){
      HierarchicalConfiguration subConfig = conf.configurationAt("manIndex[" + i + "]");
      String tableName = subConfig.getString("table");
      String newTableName = subConfig.getString("newTable");
      boolean clustered = subConfig.getBoolean("clustered");
      String indexName = subConfig.getString("indexName");
      String[] columns = subConfig.getStringArray("columns");
      copyTable(tableName, newTableName);
      createIndex(clustered,indexName,newTableName,columns);
    }
  }

  /**
   * Creates a new table containing the columns of an initial table and the columns of a datafile.
   * Initially, an intermediary table is created with a DDL statement. This table is then filled
   * during a bulk insertion. The new table is created by joining the initial table and the
   * temporary table on their keys. The intermediary table is dropped as it is now superfluous. The
   * rows of the datafile are added to the initial table by joining on the primary key of the
   * initial table and the first column of the datafile. Only the original primary key column is
   * preserved.
   *
   * @param tbl The table to which the columns are added.
   * @param newTbl The name of the newly created Table.
   * @param primaryKey Indicates the Primary Key for tbl.
   * @param columnTypes Must indicate the type for each column in the datafile.
   * @param columnNames Must indicate a name for each column in the datafile.
   * @param dataFile Can contain arbitrary number of columns. However, the first column must be a FK
   *     column for tbl.
   */
  public void updateTable(
      String tbl,
      String newTbl,
      String primaryKey,
      String[] columnTypes,
      String[] columnNames,
      String dataFile) {
    String str = "(";
    int numberOfColumns = columnTypes.length;
    for (int i = 0; i < numberOfColumns; i++) {
      String col = columnNames[i] + " " + columnTypes[i];
      str += i == numberOfColumns - 1 ? col + " " : col + ", ";
    }
    str += ")";
    try {
      Statement stmt = conn.createStatement();
      String sqlStmt = "CREATE table temporary1" + str;
      stmt.executeUpdate(sqlStmt);
      BulkInsert qNew = new BulkInsert(dataFile, "temporary1");
      qNew.update(conn);
      str = "";
      for (int i = 1; i < numberOfColumns; i++) {
        String corr = "tbl2." + columnNames[i] + " ";
        str += i == numberOfColumns - 1 ? corr + " " : corr + ", ";
      }
      String key = "tbl2." + columnNames[0];
      String sqlStmt2 =
          String.format(
              "Select tbl1.*, %s into %s from %s as tbl1, temporary1 as tbl2 where tbl1.%s = %s ",
              str, newTbl, tbl, primaryKey, key);
      stmt.executeUpdate(sqlStmt2);
      stmt.executeUpdate("DROP TABLE temporary1");
    } catch (java.sql.SQLException e) {
      e.printStackTrace();
    }
  }

  public void updateColumn(
      String tbl,
      String pk,
      String column,
      String keytype,
      String type,
      String[] colNames,
      String dataFile) {
    String newTbl = tbl + "_" + column + "updated";
    updateColumn(tbl, newTbl, pk, column, keytype, type, colNames, dataFile);
  }

  /**
   * Creates a new Table where a single column has been updated with regard to the initial table.
   * The updated column must have a different name than the original column. The initial table is
   * preserved. The new Table is created by using the updateTable function to add the updated column
   * to the table. Once this is done, the old column is dropped.
   *
   * @param tbl The name of the table for which a column is updated.
   * @param newTbl The name of the newly created table containing the updated column
   * @param pk The primary key of the table tbl.
   * @param column The name of the column which is updated.
   * @param keytype The datatype of the primary key.
   * @param type The datatype of the column that is updated.
   * @param columNames Contains the names for the updated column.
   * @param dataFile The file containing the values to update the column. The file contains two
   *     columns. The first one is an FK column for tbl, the second contains the values for the
   *     updated column.
   */
  public void updateColumn(
      String tbl,
      String newTbl,
      String pk,
      String column,
      String keytype,
      String type,
      String[] columNames,
      String dataFile) {
    try {
      String[] typeArray = {keytype, type};
      updateTable(tbl, newTbl, pk, typeArray, columNames, dataFile);
      Statement stmt = conn.createStatement();
      String sqlStmt = String.format("ALTER TABLE %s DROP COLUMN %s", newTbl, column);
      stmt.executeUpdate(sqlStmt);
    } catch (java.sql.SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * Executes an SQL statement.
   *
   * @param sqlStmt
   */
  public void update(String sqlStmt) {
    try {
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(sqlStmt);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
  public void copyTable(String tableName, String copyyTableName){
    String sqlStmt = String.format("SELECT * INTO %s FROM %s", copyyTableName, tableName);
    try{
      PreparedStatement stmt = conn.prepareStatement(sqlStmt);
      stmt.executeUpdate();
    }catch (SQLException e){
      e.printStackTrace();
    }
  }


  public void createIndex(boolean clustered, String indexName, String tableName, String[] columns){
    String cluster = clustered? "clustered" : "";
    String joinedColumns = Utils.StrArrayToString(columns,",", true);
    String sqlStmt = String.format("CREATE %s INDEX %s ON %s %s", cluster, indexName, tableName, joinedColumns);
    try{
      PreparedStatement stmt = conn.prepareStatement(sqlStmt);
      stmt.executeUpdate();
    } catch (SQLException e){
      e.printStackTrace();
    }


  }
}
