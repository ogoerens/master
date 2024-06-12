package framework.Anonymization;

import org.deidentifier.arx.ARXResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class AnonymizationStatistics {
  private HashMap<String, Integer> generalizationLevels;
  private HashMap<String, String> caseSensitivityInformation;

  public AnonymizationStatistics(ARXResult result, ArrayList<String> columns) {
    this.generalizationLevels = ARXUtils.extractGeneralizationLevels(result, columns);
  }
  public AnonymizationStatistics(HashMap<String,String> caseSensitivityInformation) {
    this.caseSensitivityInformation = caseSensitivityInformation;
  }

  public HashMap<String, String> getCaseSensitivityInformation() {
    return caseSensitivityInformation;
  }
  public HashMap<String, Integer> getGeneralizationLevels() {
    return generalizationLevels;
  }

  public String printStats() {
    StringBuilder stringBuilder = new StringBuilder();
    for (Map.Entry<String, Integer> mapEntry : generalizationLevels.entrySet()) {
      stringBuilder.append(mapEntry.getKey());
      stringBuilder.append(" : ");
      stringBuilder.append(mapEntry.getValue());
      stringBuilder.append("\n");
    }
    return stringBuilder.toString();
  }

  /*
  TODO
      * stores the generalizazion levels for each column. Necessary to anonymize the queryset afterwards
   */
}
