package framework.Anonymization;

import org.deidentifier.arx.ARXLattice;
import org.deidentifier.arx.ARXResult;
import org.deidentifier.arx.Data;
import org.deidentifier.arx.DataType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ARXUtils {
  /**
   * Converts the MSSQL Server datatypes to ARX datatypes.
   * @param SqlServerDatatype The SQL Server datatype that is converted.
   * @return The ARX datatype corresponding to the specified SQL Server datatype.
   * @throws RuntimeException
   */
  public static DataType convertSQLServerDataType(String SqlServerDatatype)
      throws RuntimeException {
    switch (SqlServerDatatype) {
      case "char":
      case "varchar":
        return DataType.STRING;
      case "int":
      case "bigint":
        return DataType.INTEGER;
      case "float":
        return DataType.DECIMAL;
      default:
        throw new RuntimeException(
            String.format(
                "SQL Server datatype '%s' has not yet been implemented to be converted",
                SqlServerDatatype));
    }
  }

  public static HashMap extractGeneralizationLevels(ARXResult result, ArrayList<String> columns) {
    final ARXLattice.ARXNode optimum = result.getGlobalOptimum();
    HashMap<String, Integer> generalizations = new HashMap<>();
    for (String col : columns) {
      int generaliztionFactor = optimum.getGeneralization(col);
      generalizations.put(col, generaliztionFactor);
    }
    return generalizations;
  }
}
