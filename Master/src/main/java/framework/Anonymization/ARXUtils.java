package framework.Anonymization;

import org.deidentifier.arx.Data;
import org.deidentifier.arx.DataType;

public class ARXUtils {
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
}
