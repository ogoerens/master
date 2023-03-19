package framework.Anonymization;

import framework.DataManager;
import framework.Inserter;
import framework.PythonLauncher;

import java.sql.Connection;
import java.sql.SQLException;

public class Synthesizer {
  private Connection connection;
  private final String storeDataFilename = "transformedData";
  private final String remainingDataTableName = "RemainingData";
  private final String storeSynthDataFilename = "SynthesizedData.csv";
  private final String synthTablename = "SynthesizedData";

  public Synthesizer(Connection connection) {
    this.connection = connection;
  }

  public void synthesize(
      String tablename, String domainLocation, String[] columnsForSynth, String programLocation)
      throws SQLException, Exception {

    Transformer t = new Transformer(this.connection);
    String[] remainingCols = {"C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_COMMENT"};

    String[] types = new String[columnsForSynth.length];
    for (int i = 0; i < columnsForSynth.length; i++) {
      types[i]= "int";
    }

    DataManager dm = new DataManager(this.connection);
    dm.createTable(storeDataFilename, types, columnsForSynth);

    t.transformAndStore(
        "SYNTH", tablename, storeDataFilename, columnsForSynth, columnsForSynth, ",", true);

  /*  PythonLauncher pythonLauncher = new PythonLauncher();

    pythonLauncher.launch(
        programLocation,
        "--dataset",
        storeDataFilename + ".csv",
        "--domain",
        domainLocation,
        "--save",
        storeSynthDataFilename);

   */

    // need to create this table
    // need to remap values
    dm.createTable(this.synthTablename, types, columnsForSynth);
    Inserter ins = new Inserter(this.connection);
    ins.insert(this.synthTablename, this.storeSynthDataFilename, true, false);

    String[] cols2 = {};
    t.transform("SYNTH", "CUSTOMER", this.remainingDataTableName, cols2, remainingCols);


  StringBuilder stringBuilder = new StringBuilder();
    for (int i = 1; i < remainingCols.length; i++) {
      String corr = "tbl2." + remainingCols[i] + " ";
      String s = i == remainingCols.length - 1 ? corr + " " : corr + ", ";
      stringBuilder.append(s);
    }
    String sqlString =
        String.format(
            "Select tbl1.*, %s into RESULT from %s as tbl1 join %s as tbl2 on %s",
            stringBuilder.toString(), this.synthTablename, this.remainingDataTableName, "tbl1.C_CUSTKEY = tbl2.C_CUSTKEY");
    dm.update(sqlString);
    // dm2.updateTable("todelete", "Synth", false, "ii", colty, selectionCols2,
    // this.remainingDataTableName,"|");

  }
}
