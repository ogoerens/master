package framework;//import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import org.apache.commons.configuration2.XMLConfiguration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class BenchConfiguration {
    private String url;
    private String user;
    private String password;

    private String driverClass;

    public BenchConfiguration (String url, String user, String password, String driverClass) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.driverClass=driverClass;
    }
    public BenchConfiguration (XMLConfiguration conf) {
        this.url = conf.getString("url");
        this.user = conf.getString("username");
        this.password = conf.getString("password");
        this.driverClass=conf.getString("driver");
    }


    public Connection makeConnection() throws SQLException {
        if (user.isEmpty()) {
            return DriverManager.getConnection(url);
        } else {
            return DriverManager.getConnection(
                    url,
                    user,
                    password
            );
        }
    }

    public void init() {
        try {
            //DriverManager.registerDriver(new SQLServerDriver());
            Class.forName(this.driverClass);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to initialize JDBC driver '" + this.driverClass + "'", ex);
        }
    }
}
