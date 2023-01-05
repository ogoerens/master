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
     * Loads Data into the datatype Data used by ARX
     * @param filename
     * @param delimiter
     * @return
     * @throws IOException
     */
    public Data loadFile(String filename, char delimiter) throws IOException {
     return Data.create(filename, AnonymizationConfiguration.charset, delimiter);
    }

    public Data loadJDBC(String url, String user, String password, String table) throws SQLException, IOException {
        DataSource source = DataSource.createJDBCSource(url,user,password,table);
        Connection connection = DriverManager.getConnection(url, user, password);
        ArrayList<String[]> columnNamesAndTypes = SQLServerUtils.getColumnNamesAndTypes(connection, table);
        connection.close();
        setColumns(source,columnNamesAndTypes);
        return  Data.create(source);
    }


    public void setColumns(DataSource source, ArrayList<String[]> columnNamesAndTypes){
        for (String[] columnAndType: columnNamesAndTypes){
            source.addColumn(columnAndType[0],ARXUtils.convertSQLServerDataType(columnAndType[1]));
        }
    }








}
