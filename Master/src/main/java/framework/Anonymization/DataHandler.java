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

public class DataHandler {
  private Data data;
  /**
   * Loads Data from a file into the ARX datatype Data.
   *
   * @param filename The name of the file that is loaded. The first line of the file should consist
   *     of the names for each attribute/column.
   * @param delimiter
   * @return
   * @throws IOException
   */
  public void loadFile(String filename, char delimiter) throws IOException {
    this.data = Data.create(filename, AnonymizationConfiguration.charset, delimiter);
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
  public void loadJDBC(String url, String user, String password, String table)
      throws SQLException, IOException {
    DataSource source = DataSource.createJDBCSource(url, user, password, table);
    Connection connection = DriverManager.getConnection(url, user, password);
    ArrayList<String[]> columnNamesAndTypes =
        SQLServerUtils.getColumnNamesAndTypes(connection, table);
    connection.close();
    addColumnsToDataSource(source, columnNamesAndTypes);
    this.data = Data.create(source);
  }

  /**
   * Adds the specified columns and their type to the datasource.
   *
   * @param source
   * @param columnNamesAndTypes
   */
  public void addColumnsToDataSource(DataSource source, ArrayList<String[]> columnNamesAndTypes) {
    for (String[] columnAndType : columnNamesAndTypes) {
      source.addColumn(columnAndType[0], ARXUtils.convertSQLServerDataType(columnAndType[1]));
    }
  }

  /**
   * @param data
   * @param hierarchies
   */
  /**
   * // Adds a hierarchy or a hierarchyBuilder to each quasi-identifying attribute/column in the
   * data.
   *
   * @param hierarchies Contains the hierarchy for each attribute.
   */
  public void addHierarchiesToData(HierarchyStore hierarchies) {
    for (String attribute : data.getDefinition().getQuasiIdentifyingAttributes()) {
      if (hierarchies.getIndexForColumnName(attribute) == 0) {
        data.getDefinition().setAttributeType(attribute, hierarchies.hierarchies.get(attribute));
      } else {
        data.getDefinition()
            .setAttributeType(attribute, hierarchies.hierarchyBuilders.get(attribute));
      }
    }
  }

  /**
   * Sets the attribute types for the attributes in the data. Attributes can be either
   * insensitive, sensitive, identifying or quasi-identifying.
   *
   * @param config Contains the information what types the attributes have.
   */
  public void applyConfigToData(AnonymizationConfiguration config) {
    for (String s : config.getInsensitiveAttributes()) {
      data.getDefinition().setAttributeType(s, AttributeType.INSENSITIVE_ATTRIBUTE);
    }
    for (String s : config.getSensitiveAttributes()) {
      data.getDefinition().setAttributeType(s, AttributeType.SENSITIVE_ATTRIBUTE);
    }
    for (String s : config.getIdentifyingAttributes()) {
      data.getDefinition().setAttributeType(s, AttributeType.IDENTIFYING_ATTRIBUTE);
    }
    for (String s : config.getQuasiIdentifyingAttributes()) {
      data.getDefinition().setAttributeType(s, AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
    }
  }

  public Data getData() {
    return data;
  }
}
