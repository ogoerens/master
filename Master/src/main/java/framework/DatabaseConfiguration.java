package framework;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import microbench.Query;
import org.apache.commons.configuration2.XMLConfiguration;

public class DatabaseConfiguration {
  private String url;
  private String user;
  private String password;

  private String driverClass;
  private String database;
  private int scalefactor =1;
  private boolean createDatabase;

  public DatabaseConfiguration(
          String url, String user, String password, String driverClass, String db, int scalefactor) {
    this.url = url;
    this.user = user;
    this.password = password;
    this.driverClass = driverClass;
    this.database = db;
    this.scalefactor =scalefactor;
  }

  public DatabaseConfiguration(XMLConfiguration conf) {
    this.url = conf.getString("url");
    this.user = conf.getString("username");
    this.password = conf.getString("password");
    this.driverClass = conf.getString("driver");
    this.createDatabase = conf.getBoolean("createDatabase");
    this.database = conf.getString("database");
    this.scalefactor = conf.getInt("scalefactor");
  }

  public String getDatabase() {
    return this.database;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }

  public int getScalefactor() {
    return scalefactor;
  }

  public Connection makeConnection() throws SQLException {
    this.init();
    Connection conn;
    if (user.isEmpty()) {
      conn = DriverManager.getConnection(url);
      useDatabase(conn);
    } else {
      conn = DriverManager.getConnection(url, user, password);
      useDatabase(conn);
    }
    return conn;
  }

  public void useDatabase(Connection conn) throws SQLException {
    if (createDatabase) {
      //TODO: check if Db does not yet exist. --> if DB_ID('microbench1sf') is not NULL  print 'db exists'
      String sqlStmt = "Create database " + this.database;
      Query createDB = new Query(sqlStmt, -1);
      System.out.println("Created DataBase "+ this.database);
      createDB.update(conn);
    }
    String sqlStmt = "Use " + this.database;
    Query useDB = new Query(sqlStmt, -1);
    useDB.update(conn);
    System.out.println("Using DataBase "+ this.database +".");
  }


  /**
   * Sets up the driver for the JDBC connection to the database.
   */
  public void init() {
    try {
      // DriverManager.registerDriver(new SQLServerDriver());
      Class.forName(this.driverClass);
    } catch (Exception ex) {
      throw new RuntimeException("Failed to initialize JDBC driver '" + this.driverClass + "'", ex);
    }
  }
}
