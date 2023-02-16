package framework.Anonymization;

import org.deidentifier.arx.ARXResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class AnonymizationStatistics {
  private HashMap<String, Integer> generalizationLevels;

  public AnonymizationStatistics(ARXResult result, ArrayList<String> columns) {
    this.generalizationLevels = ARXUtils.extractGeneralizationLevels(result, columns);
  }

  public HashMap<String, Integer> getGeneralizationLevels() {
    return generalizationLevels;
  }

  public void printStats() {
    for (Map.Entry<String, Integer> mapEntry : generalizationLevels.entrySet()) {
      System.out.println(mapEntry.getKey() + " : " + mapEntry.getValue());
    }
  }

  /*
  TODO
      * stores the generalizazion levels for each column. Necessary to anonymize the queryset afterwards
   */
}
