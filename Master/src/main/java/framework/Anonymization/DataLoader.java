package framework.Anonymization;

import org.deidentifier.arx.Data;
import org.deidentifier.arx.*;
import util.SQLServerUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;

public class DataLoader {

  /**
   * Loads Data from a file into the ARX datatype Data.
   *
   * @param filename
   * @param delimiter
   * @return
   * @throws IOException
   */
  public Data loadFile(String filename, char delimiter) throws IOException {
    return Data.create(filename, AnonymizationConfiguration.charset, delimiter);
  }

  /**
   * Loads a table from a database with the help of JDBC into the ARX datatype Data. The function
   * first creates a datasource from the database table, then adds the columns that should be loaded
   * to the source. Finally, it creates the data.
   *
   * @param url The URl for connecting to a database.
   * @param user The user who connects to the database.
   * @param password The password of the user that connects to the database.
   * @param table The table in the database that is loaded.
   * @return
   * @throws SQLException
   * @throws IOException
   */
  public Data loadJDBC(String url, String user, String password, String table)
      throws SQLException, IOException {
    DataSource source = DataSource.createJDBCSource(url, user, password, table);
    Connection connection = DriverManager.getConnection(url, user, password);
    ArrayList<String[]> columnNamesAndTypes =
        SQLServerUtils.getColumnNamesAndTypes(connection, table);
    connection.close();
    addColumnsToSource(source, columnNamesAndTypes);
    return Data.create(source);
  }

  /**
   * Adds the specified columns and their type to the datasource.
   *
   * @param source
   * @param columnNamesAndTypes
   */
  public void addColumnsToSource(DataSource source, ArrayList<String[]> columnNamesAndTypes) {
    for (String[] columnAndType : columnNamesAndTypes) {
      source.addColumn(columnAndType[0], ARXUtils.convertSQLServerDataType(columnAndType[1]));
    }
  }
}
