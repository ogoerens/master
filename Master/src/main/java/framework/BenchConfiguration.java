package framework;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import microbench.Query;
import org.apache.commons.configuration2.XMLConfiguration;

public class BenchConfiguration {
  private String url;
  private String user;
  private String password;

  private String driverClass;
  private String database;
  private Connection conn;
  private boolean createDatabase;

  public BenchConfiguration(
          String url, String user, String password, String driverClass, String db) {
    this.url = url;
    this.user = user;
    this.password = password;
    this.driverClass = driverClass;
    this.database = db;
  }

  public BenchConfiguration(XMLConfiguration conf) {
    this.url = conf.getString("url");
    this.user = conf.getString("username");
    this.password = conf.getString("password");
    this.driverClass = conf.getString("driver");
    this.createDatabase = conf.getBoolean("createDatabase");
    this.database = conf.getString("database");
  }

  public String getDatabase() {
    return this.database;
  }

  public Connection makeConnection() throws SQLException {
    if (user.isEmpty()) {
      conn = DriverManager.getConnection(url);
      useDatabase();
    } else {
      conn = DriverManager.getConnection(url, user, password);
      useDatabase();
    }
    return conn;
  }

  public void useDatabase() throws SQLException {
    if (createDatabase) {
      String sqlStmt = "Create database " + this.database;
      Query createDB = new Query(sqlStmt, -1);
      System.out.println("Created DataBase "+ this.database);
      createDB.update(conn);
    }
    String sqlStmt = "Use " + this.database;
    Query useDB = new Query(sqlStmt, -1);
    useDB.update(conn);
    System.out.println("Using DataBase "+ this.database);
  }

  // sets up the driver for the JDBC connection to the database
  public void init() {
    try {
      // DriverManager.registerDriver(new SQLServerDriver());
      Class.forName(this.driverClass);
    } catch (Exception ex) {
      throw new RuntimeException("Failed to initialize JDBC driver '" + this.driverClass + "'", ex);
    }
  }
}
