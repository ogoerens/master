package framework;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.commons.beanutils.converters.SqlDateConverter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import util.BulkInsert;
import util.GenericQuery;
import util.Utils;

public class DataManager {
  private Connection conn;
  private final String defaultFieldTerminator = "-1";

  public DataManager(Connection conn) {
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
    // Index for configurationAt method starts at 1!
    manageFile(amountFile, conf);
    manageSQL(amountSQL, conf);
    manageIndex(amountIndex, conf);
    System.out.println("Data Managing has been done!");
  }

  public void manageFile(int amount, XMLConfiguration conf) {
    for (int i = 1; i <= amount; i++) {
      HierarchicalConfiguration subConfig = conf.configurationAt("manFile[" + i + "]");
      String directory = System.getProperty("user.dir") + "/generated";
      String op = subConfig.getString("operation");
      String file = directory + "/" + subConfig.getString("fileName");
      String tbl = "";
      String newTbl = subConfig.getString("newTable");
      String pk = "";
      String[] colTypes = subConfig.getStringArray("columnTypes");
      String[] colNames = subConfig.getStringArray("columnNames");
      if (!op.equals("newTable")) {
        tbl = subConfig.getString("table");
        pk = subConfig.getString("primaryKey");
      }
      System.out.println("Working on : " + file);
      switch (op) {
        case "newTable":
          String fieldTerminator;
          String rowTerminator;
          if (subConfig.containsKey("fieldTerminator")) {
            fieldTerminator = subConfig.getString("fieldTerminator");
          } else {
            fieldTerminator = defaultFieldTerminator;
          }
          if (subConfig.containsKey("rowTerminator")) {
            rowTerminator = subConfig.getString("rowTerminator");
          } else {
            rowTerminator = "0x0A";
          }
          newTable(newTbl, file, colTypes, colNames, fieldTerminator, rowTerminator);
          break;
        case "updateTable":
          updateTable(tbl, newTbl, true, pk, colTypes, colNames, file,defaultFieldTerminator);
          break;
        case "addToTable":
          updateTable(tbl, newTbl, false, pk, colTypes, colNames, file, defaultFieldTerminator);
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
  }

  public void manageSQL(int amount, XMLConfiguration conf) {
    for (int i = 1; i <= amount; i++) {
      HierarchicalConfiguration subConfig = conf.configurationAt("manSQL[" + i + "]");
      String sqlStmt = subConfig.getString("SQLStmt");
      update(sqlStmt);
    }
  }

  public void manageIndex(int amount, XMLConfiguration conf) {
    for (int i = 1; i <= amount; i++) {
      HierarchicalConfiguration subConfig = conf.configurationAt("manIndex[" + i + "]");
      String tableName = subConfig.getString("table");
      String newTableName = subConfig.getString("newTable");
      boolean clustered = subConfig.getBoolean("clustered");
      String indexName = subConfig.getString("indexName");
      String[] columns = subConfig.getStringArray("columns");
      copyTable(tableName, newTableName);
      createIndex(clustered, indexName, newTableName, columns);
    }
  }

  /**
   * Creates and populates a new Table with the data contained in the specified file.
   *
   * @param newTableName Name of the created table.
   * @param file File containing the data to be inserted into the table.
   * @param columnTypes String array containing the column types.
   * @param columnNames String array containing the column values.
   * @param fieldTerminator String that terminates fields in the data file.
   */
  public void newTable(
      String newTableName,
      String file,
      String[] columnTypes,
      String[] columnNames,
      String fieldTerminator,
      String rowTerminator) {
    try {
      // Creat Table without contents.
      createTable(newTableName, columnTypes, columnNames);
      // Create an SQL string for the filename
      String fileSQL = "'" + file + "'";
      // Add data with a Bulk Insert statement.
      BulkInsert qNew;
      if (fieldTerminator.equals(defaultFieldTerminator)) {
        qNew = new BulkInsert(fileSQL, newTableName);
      } else {
        qNew = new BulkInsert(fileSQL, newTableName, fieldTerminator, rowTerminator);
      }
      qNew.update(conn);
      System.out.println("populated table with data");
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public void newTable(
      String newTableName,
      String file,
      String[] columnTypes,
      String[] columnNames,
      String fieldTerminator) {
    newTable(
        newTableName,
        file,
        columnTypes,
        columnNames,
        fieldTerminator,
        "0x0A");
  }

  /**
   * Executes a SQL statement that creates a new table with specified column names and types. No
   * data is inserted into the table.
   *
   * @param newTableName Name of the newly created table.
   * @param columnTypes String array containing the column types.
   * @param columnNames String array containing the column names.
   * @throws SQLException
   */
  public void createTable(String newTableName, String[] columnTypes, String[] columnNames)
      throws SQLException {
    // Format the column arrays into a String that specifies the layout of the table.
    String tableSpecifications = Utils.alternate2ArraysToString(columnNames, columnTypes, " ", ",");
    tableSpecifications = Utils.surroundWithParentheses(tableSpecifications);
    // Create the SQL statement and execute it.
    Statement stmt = conn.createStatement();
    String sqlStmt = String.format("CREATE table %s %s", newTableName, tableSpecifications);
    stmt.executeUpdate(sqlStmt);
    System.out.println("Created table: " + newTableName);
  }

  public void createTable(String newTableName, ArrayList<String[]> columnNamesAndTypes)
          throws SQLException {
    // Format the column arrays into a String that specifies the layout of the table.

    String tableSpecifications = Utils.joinArrays(columnNamesAndTypes, " ", ",");
    tableSpecifications = Utils.surroundWithParentheses(tableSpecifications);
    // Create the SQL statement and execute it.
    Statement stmt = conn.createStatement();
    String sqlStmt = String.format("CREATE table %s %s", newTableName, tableSpecifications);
    stmt.executeUpdate(sqlStmt);
    System.out.println("Created table: " + newTableName);
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
      Boolean drop,
      String primaryKey,
      String[] columnTypes,
      String[] columnNames,
      String dataFile,
      String fieldTerminator
  ) {
    // Drop columns in original table.
    if (drop) {
      StringBuilder colnamesForDrop = new StringBuilder();
      for (int i = 1; i < columnNames.length; i++) {
        colnamesForDrop.append("column ");
        colnamesForDrop.append(columnNames[i]);
        if (i != columnNames.length - 1) {
          colnamesForDrop.append(",");
        }
      }
      try {
        Statement stmt = conn.createStatement();
        String sqlStmt =
            String.format("ALTER TABLE %s DROP %s", newTbl, colnamesForDrop.toString());
        stmt.executeUpdate(sqlStmt);
      } catch (java.sql.SQLException e) {
        e.printStackTrace();
      }
    }
    // Create temporary table with file content.
    newTable("temporary1", dataFile, columnTypes, columnNames, fieldTerminator);
    StringBuilder stringBuilder = new StringBuilder();
    int numberOfColumns = columnTypes.length;
    for (int i = 1; i < numberOfColumns; i++) {
      String corr = "tbl2." + columnNames[i] + " ";
      String s = i == numberOfColumns - 1 ? corr + " " : corr + ", ";
      stringBuilder.append(s);
    }
    String key = "tbl2." + columnNames[0];
    String sqlStmt2 =
        String.format(
            "Select tbl1.*, %s into %s from %s as tbl1, temporary1 as tbl2 where tbl1.%s = %s ",
            stringBuilder.toString(), newTbl, tbl, primaryKey, key);
    try {
      Statement stmt = conn.createStatement();
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
      updateTable(tbl, newTbl, false, pk, typeArray, columNames, dataFile, defaultFieldTerminator);
      Statement stmt = conn.createStatement();
      String sqlStmt = String.format("ALTER TABLE %s DROP COLUMN %s", newTbl, column);
      stmt.executeUpdate(sqlStmt);
      sqlStmt = String.format("EXEC sp_RENAME '%s.%s' , '%s', 'COLUMN'", newTbl, columNames[1], column);
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

  public void copyTable(String tableName, String copyyTableName) {
    String sqlStmt = String.format("SELECT * INTO %s FROM %s", copyyTableName, tableName);
    try {
      PreparedStatement stmt = conn.prepareStatement(sqlStmt);
      stmt.executeUpdate();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public void createIndex(boolean clustered, String indexName, String tableName, String[] columns) {
    String cluster = clustered ? "clustered" : "";
    String joinedColumns = Utils.StrArrayToString(columns, ",", true);
    String sqlStmt =
        String.format("CREATE %s INDEX %s ON %s %s", cluster, indexName, tableName, joinedColumns);
    try {
      PreparedStatement stmt = conn.prepareStatement(sqlStmt);
      stmt.executeUpdate();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
