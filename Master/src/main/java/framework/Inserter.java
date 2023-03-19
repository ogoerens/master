package framework;

import jdk.jshell.execution.Util;
import org.apache.commons.lang.StringUtils;
import util.StringUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Scanner;

public class Inserter {
  private Connection connection;
  public Inserter( Connection connection){
    this.connection = connection;
  }
  private static String csvDelimiter = ",|\\n";

  public void insert(
      String tablename,
      String filename,
      boolean withHeader,
      boolean withIndex)
      throws SQLException, FileNotFoundException {
    File file = new File(filename);
    Scanner scanner = new Scanner(file);
    scanner.useDelimiter(csvDelimiter);

    String first = scanner.nextLine();
    int columnCount = StringUtils.countMatches(first, ",") + 1;
    int columnCountIndex = columnCount;
    if (withIndex) {
      columnCountIndex++;
    }

    connection.setAutoCommit(false);
    String columnPlaceHolders = "?,".repeat(columnCountIndex);
    String storeQueryString =
        String.format(
            "INSERT INTO %s VALUES (%s)",
            tablename, columnPlaceHolders.substring(0, columnPlaceHolders.length() - 1));
    PreparedStatement storeStatement = connection.prepareStatement(storeQueryString);

    String[] firstARR = first.split(",");
    int counter = 1;

    if (!withHeader) {
      for (int i = 1; i <= firstARR.length; i++) {
        String element = firstARR[i - 1];
        storeStatement.setString(i, element);
      }
      if (withIndex) {
        storeStatement.setString(columnCountIndex, Integer.toString(counter));
        counter++;
      }
      storeStatement.addBatch();
    }

    while (scanner.hasNext()) {
      for (int i = 1; i <= columnCount; i++) {
        String element = scanner.next();
        storeStatement.setString(i, element);
      }
      if (withIndex) {
        storeStatement.setString(columnCountIndex, Integer.toString(counter));
        counter++;
      }
      storeStatement.addBatch();
    }
    storeStatement.executeBatch();
    connection.commit();
  }


}
