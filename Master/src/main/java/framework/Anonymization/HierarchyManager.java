package framework.Anonymization;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.deidentifier.arx.ARXResult;
import org.deidentifier.arx.AttributeType.Hierarchy;
import org.deidentifier.arx.DataType;
import org.deidentifier.arx.aggregates.HierarchyBuilderIntervalBased;
import org.deidentifier.arx.aggregates.HierarchyBuilderRedactionBased;
import org.deidentifier.arx.aggregates.HierarchyBuilder;
import util.Utils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;

public class HierarchyManager {
  private static final HierarchyBuilderRedactionBased.Order defaultAlignmentOrder =
      HierarchyBuilderRedactionBased.Order.LEFT_TO_RIGHT;
  private static final HierarchyBuilderRedactionBased.Order defaultRedactionOrder =
      HierarchyBuilderRedactionBased.Order.LEFT_TO_RIGHT;
  private static final char defaultPaddingCharacter = ' ';
  private static final char defaultMaskingCharacter = '*';
  private static final String hierarchyDirectory = "Hierarchies2";

  /*
  TODO:
      * public ArrayList<Hierarchies> buildHierarchies(){}
          builds all hierarchies and returns them
   */

  public HierarchyStore buildHierarchies(XMLConfiguration config) throws Exception {
    HashMap<String, Hierarchy> hierarchies = new HashMap<>();
    HashMap<String, HierarchyBuilder> hierarchyBuilders = new HashMap<>();
    int amountFile = 0;
    int amountLogic = 0;
    if (config.containsKey("amountFile")) {
      amountFile = config.getInt("amountFile");
    }
    if (config.containsKey("amountLogic")) {
      amountLogic = config.getInt("amountLogic");
    }

    if (amountFile + amountLogic == 0) {
      String errorMsg =
          "The hierarchy configuration does not contain the mandatory amount variables (amountFile, amountLogic) indicating the number of hierarchies that should be created or they are both set to 0";
      throw new Exception(errorMsg);
    }
    for (int i = 1; i <= amountFile; i++) {
      HierarchicalConfiguration subConfig = config.configurationAt("hierarchyFile[" + i + "]");
      String file = subConfig.getString("file");
      String columnName = subConfig.getString("columnName");
      hierarchies.put(
          columnName,
          buildHierarchyFromFile(
              file,
              AnonymizationConfiguration.charset,
              AnonymizationConfiguration.hierarchyValueDelimiter));
    }

    for (int i = 1; i <= amountLogic; i++) {
      HierarchicalConfiguration subConfig = config.configurationAt("hierarchyLogic[" + i + "]");
      String logic = subConfig.getString("logic");
      String columnName = subConfig.getString("columnName");
      hierarchyBuilders.put(columnName, buildHierarchyFromLogic(logic, subConfig));
    }

    return new HierarchyStore(hierarchies, hierarchyBuilders);
  }

  /**
   * Creates a Hierarchy from a CSV File
   *
   * @param filename
   * @param charset
   * @param delimiter
   * @return
   * @throws IOException
   */
  public Hierarchy buildHierarchyFromFile(String filename, Charset charset, char delimiter)
      throws IOException {
    return Hierarchy.create(filename, charset, delimiter);
  }

  public HierarchyBuilder<?> buildHierarchyFromLogic(
      String logic, HierarchicalConfiguration subconfig) throws Exception {
    switch (logic) {
      case "mask":
        return buildMaskHierarchyBuilder(subconfig);
      case "interval":
        return buildIntervalHierarchyBuilder(subconfig);
      default:
        String errorMsg = String.format("The indicated Hierarchy logic '%s' was not found.", logic);
        throw new Exception(errorMsg);
    }
  }

  /**
   * Creates a HierarchyBuilder that masks String either starting from left or right. The specific
   * order is defined by the redactionOrder variable. The redaction order defines if the Strings are
   * padded to the left or right with whitespace such that each String has the same length.
   *
   * @param subconfig
   * @return
   */
  private HierarchyBuilderRedactionBased<?> buildMaskHierarchyBuilder(
      HierarchicalConfiguration subconfig) {
    HierarchyBuilderRedactionBased.Order alignmentOrder = defaultAlignmentOrder;
    HierarchyBuilderRedactionBased.Order redactionOrder = defaultRedactionOrder;
    if (subconfig.containsKey("alignmentOrder")) {
      if (subconfig.getString("alignmentOrder").equals("rightToLeft")) {
        alignmentOrder = HierarchyBuilderRedactionBased.Order.RIGHT_TO_LEFT;
      }
      if (subconfig.getString("redactionOrder").equals("rightToLeft")) {
        redactionOrder = HierarchyBuilderRedactionBased.Order.RIGHT_TO_LEFT;
      }
    }
    HierarchyBuilderRedactionBased<?> builder =
        HierarchyBuilderRedactionBased.create(
            alignmentOrder, redactionOrder, defaultPaddingCharacter, defaultMaskingCharacter);
    return builder;
  }

  private HierarchyBuilderIntervalBased<?> buildIntervalHierarchyBuilder(
      HierarchicalConfiguration subconfig) {
    int intervalLength = subconfig.getInt("intervalLength");
    long lowerbound = subconfig.getInt("lowerbound");
    long upperbound = subconfig.getInt("upperbound");
    int groupingAmount = subconfig.getInt("groupingAmount");

    HierarchyBuilderIntervalBased<?> intervalBasedBuilder;
    if (subconfig.getString("datatype").equals("integer")) {
      intervalBasedBuilder = buildIntervalHierarchyBuilderInteger(lowerbound, upperbound, intervalLength);
    } else {
      intervalBasedBuilder = buildIntervalHierarchyBuilderDouble((double) lowerbound, (double) upperbound, intervalLength);
    }
    double numberOfLevels =
        Math.log((upperbound - lowerbound) / intervalLength) / Math.log(groupingAmount);
    for (int i = 0; i < numberOfLevels; i++) {
      intervalBasedBuilder.getLevel(i).addGroup(groupingAmount);
    }
    return intervalBasedBuilder;
  }

  private HierarchyBuilderIntervalBased<Double> buildIntervalHierarchyBuilderDouble(
      double lowerboundD, double upperboundD, int intervalLength) {
    HierarchyBuilderIntervalBased<Double> intervalBasedBuilder =
        HierarchyBuilderIntervalBased.create(
            DataType.DECIMAL,
            new HierarchyBuilderIntervalBased.Range<Double>(lowerboundD, lowerboundD, lowerboundD),
            new HierarchyBuilderIntervalBased.Range<Double>(upperboundD, upperboundD, upperboundD));
    intervalBasedBuilder.addInterval(lowerboundD, lowerboundD + intervalLength);
    intervalBasedBuilder.setAggregateFunction(
        DataType.DECIMAL.createAggregate().createIntervalFunction(true, true));

    return intervalBasedBuilder;
  }

  private HierarchyBuilderIntervalBased<Long> buildIntervalHierarchyBuilderInteger(
      long lowerbound, long upperbound, int intervalLength) {
    HierarchyBuilderIntervalBased<Long> intervalBasedBuilder =
        HierarchyBuilderIntervalBased.create(
            DataType.INTEGER,
            new HierarchyBuilderIntervalBased.Range<Long>(lowerbound, lowerbound, lowerbound),
            new HierarchyBuilderIntervalBased.Range<Long>(upperbound, upperbound, upperbound));
    intervalBasedBuilder.addInterval(lowerbound, lowerbound + intervalLength);
    intervalBasedBuilder.setAggregateFunction(
        DataType.INTEGER.createAggregate().createIntervalFunction(true, true));

    return intervalBasedBuilder;
  }

  public static String getHierarchyDirectory() {
    return hierarchyDirectory;
  }

  /**
   * Stores a nested String array in an indicated file. Each inner array is printed to anew line.
   * Elements from inner arrays are seperated by a specified character.
   *
   * @param array
   * @param fileName
   */
  public static void storeHierarchy(String[][] array, String fileName) {
    Utils.storeNestedArray(array, fileName, "\n", ";");
  }

  public static void storeHierarchy(ARXResult arxResult, String hierarchyName, String fileName) {
    storeHierarchy(arxResult.getDataDefinition().getHierarchy(hierarchyName), fileName);
  }
}
