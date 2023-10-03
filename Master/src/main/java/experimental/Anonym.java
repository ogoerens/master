package experimental;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.*;

import framework.Anonymization.HierarchyManager;
import framework.Anonymization.HierarchyStore;
import org.apache.commons.configuration2.XMLConfiguration;
import org.deidentifier.arx.*;
import org.deidentifier.arx.AttributeType.Hierarchy;
import org.deidentifier.arx.AttributeType.Hierarchy.DefaultHierarchy;
import org.deidentifier.arx.aggregates.*;
import org.deidentifier.arx.criteria.KAnonymity;
import util.Utils;

public class Anonym {
  public void work() throws IOException, java.lang.ClassNotFoundException, java.sql.SQLException {

    /*
    Query Anonimization: Parser
     String qqq =
        "SELECT Top(100)\n"
            + "     s_acctbal,\n"
            + "     s_name,\n"
            + "     n_name,\n"
            + "     p_partkey,\n"
            + "     p_mfgr,\n"
            + "     s_address,\n"
            + "     s_phone,\n"
            + "     s_comment\n"
            + " FROM part, supplier, partsupp, nation, region\n"
            + "WHERE\n"
            + "     p_partkey = ps_partkey\n"
            + "     AND s_suppkey = ps_suppkey\n"
            + "     AND p_size = 15\n"
            + "     AND p_type LIKE '%BRASS'\n"
            + "     AND s_nationkey = n_nationkey\n"
            + "     AND n_regionkey = r_regionkey\n"
            + "     AND r_name = 'EUROPE'";
    String qqq2 = Queries.q3a;
    String qqq15 = Queries.q15;
    experimental.Parser p = new experimental.Parser();
    p.parse(qqq);
    p.parse(qqq2);
    p.parse(qqq15);
     */

    HierarchyBuilderOrderBased<Long> builder =
        HierarchyBuilderOrderBased.create(DataType.INTEGER, false);

    // Define grouping fanouts
    builder.getLevel(0).addGroup(5, DataType.INTEGER.createAggregate().createIntervalFunction());
    builder.getLevel(1).addGroup(1, DataType.INTEGER.createAggregate().createIntervalFunction());
    builder.getLevel(2).addGroup(2, DataType.INTEGER.createAggregate().createIntervalFunction());

    System.out.println("---------------------");
    System.out.println("ORDER-BASED HIERARCHY");
    System.out.println("---------------------");
    System.out.println("");
    System.out.println("SPECIFICATION");

    // Print specification
    for (HierarchyBuilderGroupingBased.Level<Long> level : builder.getLevels()) {
      System.out.println(level);
    }

    // Print info about resulting groups
    System.out.println("Resulting levels: " + Arrays.toString(builder.prepare(getExampleData())));

    System.out.println("");
    System.out.println("RESULT");

    // Print resulting hierarchy
    printArray(builder.build().getHierarchy());
    System.out.println("");

    HierarchyBuilderRedactionBased<?> builderr =
        HierarchyBuilderRedactionBased.create(
            HierarchyBuilderRedactionBased.Order.LEFT_TO_RIGHT,
            HierarchyBuilderRedactionBased.Order.RIGHT_TO_LEFT,
            ' ',
            '*');

    System.out.println("-------------------------");
    System.out.println("REDACTION-BASED HIERARCHY");
    System.out.println("-------------------------");
    System.out.println("");
    System.out.println("SPECIFICATION");

    // Print info about resulting groups
    System.out.println("Resulting levels: " + Arrays.toString(builderr.prepare(getExampleData())));

    System.out.println("");
    System.out.println("RESULT");

    // Print resulting hierarchy
    printArray(builderr.build().getHierarchy());
    System.out.println("");

    HierarchyBuilderIntervalBased<Long> builder2 =
        HierarchyBuilderIntervalBased.create(
            DataType.INTEGER,
            new HierarchyBuilderIntervalBased.Range<Long>(0l, 0l, Long.MIN_VALUE / 4),
            new HierarchyBuilderIntervalBased.Range<Long>(110l, 110L, Long.MAX_VALUE / 4));

    // Define base intervals
    builder2.setAggregateFunction(
        DataType.INTEGER.createAggregate().createIntervalFunction(true, false));
    builder2.addInterval(0l, 5l);
    // builder2.addInterval(10l, 15l);

    // Define grouping fanouts
    double x = Math.log((110 - 0) / 5) / Math.log(2);
    System.out.println(x);
    for (int i = 0; i < (x); i++) {
      builder2.getLevel(i).addGroup(2);
    }

    System.out.println("------------------------");
    System.out.println("INTERVAL-BASED HIERARCHY");
    System.out.println("------------------------");
    System.out.println("SPECIFICATION");

    // Print specification
    for (HierarchyBuilderIntervalBased.Interval<Long> interval : builder2.getIntervals()) {
      System.out.println(interval);
    }

    // Print specification
    for (HierarchyBuilderGroupingBased.Level<Long> level : builder2.getLevels()) {
      System.out.println(level);
    }

    // Print info about resulting levels
    System.out.println("Resulting levels: " + Arrays.toString(builder2.prepare(getExampleData())));

    System.out.println("");
    System.out.println("RESULT");

    // Print resulting hierarchy
    printArray(builder2.build().getHierarchy());
    System.out.println("");

    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

    DataSource source =
        DataSource.createJDBCSource(
            "jdbc:sqlserver://localhost:1433;encrypt=false;database=test;",
            "sa",
            ".+.QET21adg.+.",
            "testdata");
    source.addColumn(2, DataType.STRING); // zipcode (index based addressing)
    source.addColumn(1, DataType.STRING); // gender (named addressing)
    source.addColumn(0, "renamed", DataType.INTEGER);
    source.addColumn(3, DataType.STRING);

    Data data = Data.create(source);

    Data data2 = Data.create("testdata.csv", StandardCharsets.UTF_8, ',');

    /*
    DefaultData data = Data.create();
    data.add("age", "gender", "zipcode","illness");
    data.add("34", "male", "81667","Flu");
    data.add("45", "female", "81675","Flu");
    data.add("66", "male", "81925","Flu");
    data.add("70", "female", "81931","Flu");
    data.add("34", "female", "81931","Cancer");
    data.add("70", "male", "81931","Cancer");
    data.add("45", "male", "81931","Cancer");
     */

    // Define hierarchies
    DefaultHierarchy age = Hierarchy.create();
    age.add("34", "<40", "<50");
    age.add("45", "40<x<=50", "<50");
    age.add("66", ">50", ">50");
    age.add("70", ">50", ">50");

    /*DefaultHierarchy gender = Hierarchy.create();
       gender.add("male", "*");
       gender.add("female", "*");

    */
    String hierarchiesFile =
        "/home/olivier/Documents/MasterThesis/Master/src/main/resources/hierarchies.xml";
    XMLConfiguration hierarchyConf = Utils.buildXMLConfiguration(hierarchiesFile);
    HierarchyManager hierarchyBuilder = new HierarchyManager(hierarchyConf);

    HierarchyStore hierarchies = new HierarchyStore();
    try {
      hierarchyBuilder.buildHierarchies();
      hierarchies = hierarchyBuilder.getHierarchyStore();
    } catch (Exception e) {
      e.printStackTrace();
    }

    for (String s : hierarchies.getColumnNames()) {
      if (hierarchies.getIndexForColumnName(s) == 0) {
        data.getDefinition().setAttributeType(s, hierarchies.getHierarchies().get(s));
      } else {
        data.getDefinition().setAttributeType(s, hierarchies.getHierarchyBuilders().get(s));
      }
    }

    // Only excerpts for readability
    DefaultHierarchy zipcode = Hierarchy.create();
    zipcode.add("81667", "8166*", "816**", "81***", "8****", "*****");
    zipcode.add("81675", "8167*", "816**", "81***", "8****", "*****");
    zipcode.add("81925", "8192*", "819**", "81***", "8****", "*****");
    zipcode.add("81931", "8193*", "819**", "81***", "8****", "*****");
    /*
           data.getDefinition().setAttributeType("age", age);
           data.getDefinition().setAttributeType("gender", gender);
           data.getDefinition().setAttributeType("zipcode", zipcode);
    */

    // data.getDefinition().setAttributeType("renamed", age);
    // data.getDefinition().setAttributeType("gender", gender);
    // data.getDefinition().setAttributeType("zipcode", zipcode2);

    data.getDefinition().setAttributeType("illness", AttributeType.INSENSITIVE_ATTRIBUTE);

    // Create an instance of the anonymizer
    ARXAnonymizer anonymizer = new ARXAnonymizer();
    ARXConfiguration config = ARXConfiguration.create();
    config.addPrivacyModel(new KAnonymity(1));
    // config.addPrivacyModel(new DistinctLDiversity("illness", 2));

    // NDS-specific settings
    /*
    config.setSuppressionLimit(1d); // Recommended default: 1d
    config.setAttributeWeight("age", 0.5d); // attribute weight
    config.setAttributeWeight("gender", 0.3d); // attribute weight
    config.setAttributeWeight("zipcode", 0.5d); // attribute weight

    config.setQualityModel(Metric.createLossMetric(0.5d)); // suppression/generalization-factor

     */

    hierarchies.getHierarchyBuilders().get("c_custkey");
    if (hierarchies.getHierarchyBuilders().get("c_custkey")
        instanceof HierarchyBuilderIntervalBased<?>) {
      System.out.println("ohje");
    }
    HierarchyBuilderIntervalBased intervalBased =
        (HierarchyBuilderIntervalBased) hierarchies.getHierarchyBuilders().get("c_custkey");

    long topval = (long) intervalBased.getUpperRange().getBottomTopCodingFrom() - 1;

    // data1.getDefinition().setMinimumGeneralization();

    String[] vals = {
      "0",
      "11",
      intervalBased.getLowerRange().getBottomTopCodingFrom().toString(),
      intervalBased.getUpperRange().getBottomTopCodingFrom().toString()
    };
    intervalBased.prepare(vals);
    printArray(intervalBased.build().getHierarchy());

    String[] valss = {"%a%b%cdef", "%xsafasfx", "%xsafafx%", "asfasfasf%"};
    if (hierarchies.getHierarchyBuilders().get("c_name")
        instanceof HierarchyBuilderRedactionBased<?>) {
      System.out.println("ohneee");
      HierarchyBuilderRedactionBased redactionBased =
          (HierarchyBuilderRedactionBased) hierarchies.getHierarchyBuilders().get("c_name");
      int size = 4; // found by hiera[0][1].length.

      for (String originalVal : valss) {
        int generalizationLevel = 3;
        int remainingLength = size - generalizationLevel;
        String modifiedValue = originalVal.replace("%", "");
        int valSize = modifiedValue.length();

        String newVal = modifiedValue;
        if (valSize > size) {
          modifiedValue.substring(0, size);
        }

        String anonVal = "";
        ArrayList<Integer> indexes = new ArrayList<>();
        int index = originalVal.indexOf("%");
        while (index >= 0) {
          indexes.add(index);
          index = originalVal.indexOf("%", index + 1);
        }

        // Mask right to left
        int k = 0;
        StringBuilder stringBuilder = new StringBuilder();
        for (int i=0; i<originalVal.length(); i++){
          if (k==size){
            if (originalVal.charAt(i)=='%'){
              stringBuilder.append('%');
            }
            break;
          }
          stringBuilder.append(originalVal.charAt(i));
          if (originalVal.charAt(i)!='%'){
            k++;
          }

        }
        System.out.println(stringBuilder.toString());

        k = 0;
        StringBuilder otherWay = new StringBuilder();
        for (int i = 1; i <= originalVal.length(); i++) {
          if (k < size) {
            otherWay.append(originalVal.charAt(originalVal.length() - i));
            if (originalVal.charAt(originalVal.length() - i) != '%') {
              k++;
            }
          } else {
            if (originalVal.charAt(originalVal.length() - i) == '%') {
              otherWay.append("%");
            }
            break;
          }
        }
        otherWay.reverse();
        System.out.println(otherWay.toString());
      }

      HierarchyBuilderRedactionBased.Order order = redactionBased.getRedactionOrder();
    }

    /*
    try {
      ARXResult result = anonymizer.anonymize(data1, config);

      // Print info
      printResult(result, data1);
      // Process results
      System.out.println(" - Transformed data:");
      Iterator<String[]> transformed = result.getOutput(false).iterator();
      while (transformed.hasNext()) {
        System.out.print("   ");
        System.out.println(Arrays.toString(transformed.next()));
      }
    } catch (java.io.IOException e) {
      e.printStackTrace();
    }

     */
  }

  private static String[] getExampleData() {

    String[] result = new String[101];
    for (int i = 0; i < result.length; i++) {
      result[i] = String.valueOf(i * 2);
    }
    return result;
  }

  public static void printResult(final ARXResult result, final Data data) {

    // Print time
    final DecimalFormat df1 = new DecimalFormat("#####0.00");
    final String sTotal = df1.format(result.getTime() / 1000d) + "s";
    System.out.println(" - Time needed: " + sTotal);

    // Extract
    final ARXLattice.ARXNode optimum = result.getGlobalOptimum();
    final List<String> qis =
        new ArrayList<String>(data.getDefinition().getQuasiIdentifyingAttributes());

    if (optimum == null) {
      System.out.println(" - No solution found!");
      return;
    }

    // Initialize
    final StringBuffer[] identifiers = new StringBuffer[qis.size()];
    final StringBuffer[] generalizations = new StringBuffer[qis.size()];
    int lengthI = 0;
    int lengthG = 0;
    for (int i = 0; i < qis.size(); i++) {
      identifiers[i] = new StringBuffer();
      generalizations[i] = new StringBuffer();
      identifiers[i].append(qis.get(i));
      generalizations[i].append(optimum.getGeneralization(qis.get(i)));
      if (data.getDefinition().isHierarchyAvailable(qis.get(i)))
        generalizations[i]
            .append("/")
            .append(data.getDefinition().getHierarchy(qis.get(i))[0].length - 1);
      lengthI = Math.max(lengthI, identifiers[i].length());
      lengthG = Math.max(lengthG, generalizations[i].length());
    }

    // Padding
    for (int i = 0; i < qis.size(); i++) {
      while (identifiers[i].length() < lengthI) {
        identifiers[i].append(" ");
      }
      while (generalizations[i].length() < lengthG) {
        generalizations[i].insert(0, " ");
      }
    }

    // Print
    System.out.println(
        " - Information loss: "
            + result.getGlobalOptimum().getLowestScore()
            + " / "
            + result.getGlobalOptimum().getHighestScore());
    System.out.println(" - Optimal generalization");
    for (int i = 0; i < qis.size(); i++) {
      System.out.println("   * " + identifiers[i] + ": " + generalizations[i]);
    }
    System.out.println(" - Statistics");
    System.out.println(
        result
            .getOutput(result.getGlobalOptimum(), false)
            .getStatistics()
            .getEquivalenceClassStatistics());
  }

  public static void printArray(String[][] array) {
    System.out.print("{");
    for (int j = 0; j < array.length; j++) {
      String[] next = array[j];
      System.out.print("{");
      for (int i = 0; i < next.length; i++) {
        String string = next[i];
        System.out.print("\"" + string + "\"");
        if (i < next.length - 1) {
          System.out.print(",");
        }
      }
      System.out.print("}");
      if (j < array.length - 1) {
        System.out.print(",\n");
      }
    }
    System.out.println("}");
  }

  public void storeHierarchy(String[][] array, String fileName) {
    StringBuffer stringBuffer = new StringBuffer();
    for (int j = 0; j < array.length; j++) {
      String[] next = array[j];
      for (int i = 0; i < next.length; i++) {
        String string = next[i];
        stringBuffer.append(string);
        if (i < next.length - 1) {
          stringBuffer.append(";");
        }
      }
      if (j < array.length - 1) {
        stringBuffer.append(",\n");
      }
    }
    Utils.strToFile(stringBuffer.toString(), fileName);
  }
}
