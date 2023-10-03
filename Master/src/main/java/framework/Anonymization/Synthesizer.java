package framework.Anonymization;

import framework.DataManager;
import framework.Inserter;
import framework.PythonLauncher;
import microbench.Query;
import util.SQLServerUtils;
import util.Utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class Synthesizer {
  private Connection connection;
  private final String storeDataFilename = "transformedData.csv";
  private final String modifiedTableName = "modifiedData";
  private final String mainTableName = "mainData";
  private final String remainingTableName = "RemainingData";
  private final String storeSynthDataFilename =
      System.getProperty("user.dir") + "/SynthesizedData.csv";
  private final String synthTablename = "SynthesizedData";

  public Synthesizer(Connection connection) {
    this.connection = connection;
  }

  public void synthesize(AnonymizationConfiguration anonconfig, String programLocation)
      throws SQLException, Exception {

    String tablename = anonconfig.getDataTableName();
    String domainLocation = anonconfig.getDomainFileLocation();
    String[] columnsForSynth = anonconfig.getColumnsForSynth();
    String[] remainingCols = anonconfig.getRemainingColumns();

    Transformer t = new Transformer(this.connection);
    DataManager dm = new DataManager(this.connection);

    String[] selectionCols = {"*"};
    // Maps the customer dataset to a discret customer set in the attributes thare synthesized.
    t.transform("SYNTH", tablename, modifiedTableName, columnsForSynth, selectionCols);
    // Projects two tables. One only containing the attributes thare synthesized. One containin the
    // remaining columns and a key column used to join both tables later.
    String[] emptyCols = {};
    t.transform("project", modifiedTableName, this.remainingTableName, emptyCols, remainingCols);
    t.transform("project", modifiedTableName, this.mainTableName, emptyCols, columnsForSynth);

    //Stores the table containing only the synthesized columns in a file. Needed for the MST algorithm.
    String header = Utils.StrArrayToString(columnsForSynth, ",", false) + System.lineSeparator();
    String storeStmt = String.format("Select * from %s", mainTableName);
    Query storeQuery = new Query(storeStmt, "Synthesizer-StoreQuery");
    storeQuery.runAndStore(connection, columnsForSynth.length, ",", this.storeDataFilename, header);

    // Runs the MST algorithm.
    PythonLauncher pythonLauncher = new PythonLauncher();

    pythonLauncher.launch(
        programLocation,
        "--dataset",
        storeDataFilename,
        "--domain",
        domainLocation,
        "--save",
        storeSynthDataFilename);

    // need to create this table
    // need to remap values
    String[] types = new String[columnsForSynth.length];
    for (int i = 0; i < columnsForSynth.length; i++) {
      types[i] = "int";
    }
    dm.createTable(this.synthTablename, types, columnsForSynth);

    Inserter ins = new Inserter(this.connection);
    ins.insert(this.synthTablename, this.storeSynthDataFilename, true, false);

    // dm.newTable(this.synthTablename,this.storeSynthDataFilename,types,
    // columnsForSynth,",",System.lineSeparator());
    // does not work because of header.

    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 1; i < remainingCols.length; i++) {
      String corr = "tbl2." + remainingCols[i] + " ";
      String s = i == remainingCols.length - 1 ? corr + " " : corr + ", ";
      stringBuilder.append(s);
    }
    String sqlString =
        String.format(
            "Select tbl1.*, %s into RESULT from %s as tbl1 join %s as tbl2 on %s",
            stringBuilder.toString(),
            this.synthTablename,
            this.remainingTableName,
            "tbl1.C_CUSTKEY = tbl2.C_CUSTKEY");
    dm.update(sqlString);
    // dm2.updateTable("todelete", "Synth", false, "ii", colty, selectionCols2,
    // this.remainingDataTableName,"|");

  }

  private void maptoDiscreteCustomer(Connection conn) throws SQLException {
    Transformer transformer = new Transformer(conn);
    String[] selectionCols = {"*"};
    transformer.transform("SYNTH", "customer", "discreteCustomer", selectionCols, selectionCols);
  }

  private void maptoOriginal(Connection conn, String outputTableName) throws SQLException {
    Transformer transformer = new Transformer(conn);
    String[] cols = {"*"};
    transformer.transform(
        "SYNTHBACK",
        "RESULT",
        outputTableName,
        cols,
        cols,
        util.SQLServerUtils.getColumnNamesAndTypes(conn, "customer"));
  }
}
