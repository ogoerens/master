package framework.Anonymization;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.deidentifier.arx.AttributeType.Hierarchy;
import org.deidentifier.arx.DataType;
import org.deidentifier.arx.aggregates.HierarchyBuilderIntervalBased;
import org.deidentifier.arx.aggregates.HierarchyBuilderRedactionBased;
import org.deidentifier.arx.aggregates.HierarchyBuilder;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;

public class HierarchyManager {
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
      case "maskRightToLeft":
        HierarchyBuilderRedactionBased<?> builder =
            HierarchyBuilderRedactionBased.create(
                HierarchyBuilderRedactionBased.Order.RIGHT_TO_LEFT,
                HierarchyBuilderRedactionBased.Order.RIGHT_TO_LEFT,
                ' ',
                '*');
        return builder;
      case "maskLeftToRight":
        builder =
            HierarchyBuilderRedactionBased.create(
                HierarchyBuilderRedactionBased.Order.LEFT_TO_RIGHT,
                HierarchyBuilderRedactionBased.Order.LEFT_TO_RIGHT,
                ' ',
                '*');
        return builder;
      case "interval":
        int interval = subconfig.getInt("intervalLength");
        long lowerbound = subconfig.getInt("lowerbound");
        long upperbound = subconfig.getInt("upperbound");
        HierarchyBuilderIntervalBased<Long> intervalBasedBuilder =
            HierarchyBuilderIntervalBased.create(
                DataType.INTEGER,
                new HierarchyBuilderIntervalBased.Range<Long>(
                    lowerbound, lowerbound, Long.MIN_VALUE / 4),
                new HierarchyBuilderIntervalBased.Range<Long>(
                    upperbound, upperbound, Long.MAX_VALUE / 4));

        intervalBasedBuilder.addInterval(lowerbound, upperbound + interval);
        intervalBasedBuilder.setAggregateFunction(
            DataType.INTEGER.createAggregate().createIntervalFunction(true, false));
        intervalBasedBuilder.getLevel(0).addGroup(4);

        return intervalBasedBuilder;
        // interval length
        // number levels
        // lower bound and upper bound

      default:
        String errorMsg = String.format("The indicated Hierarchy logic '%s' was not found.", logic);
        throw new Exception(errorMsg);
    }
  }
}
