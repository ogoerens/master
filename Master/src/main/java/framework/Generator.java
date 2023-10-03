package framework;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Random;

import microbench.MicrobenchUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.distribution.PoissonDistribution;
import util.Utils;

public class Generator {

  private Random rand;
  private final String storagefolder = System.getProperty("user.dir") + "/generated/";
  private final int firstLetter = 'a';
  private final int lastLetter = 'z';
  private final int numberOfChars = lastLetter - firstLetter;

  public Generator(Random r) {
    this.rand = r;
  }

  /**
   * Analyzes an XML configuration file and generates datasets as specified in the configuration
   * file. Each generated datasets is stored in a file, with the filename specified in the
   * configuration.
   *
   * @param conf Configuration that contains all the information needed to generate the datasets.
   *     Must contain an integer indicating the number of dataset generations and a default size for
   *     each dataset.
   */
  public void generate(XMLConfiguration conf) {
    // Create directory ("generated") containing all datasets if it does not yet exist.
    try {
      Files.createDirectories(Paths.get("generated"));
    } catch (IOException e) {
      e.printStackTrace();
    }
    // Retrieve the common arguments amount and size.
    int amount = conf.getInt("amount");
    int size = conf.getInt("defaultSize");
    // Create subconfiguration for all dataset and handle them one-by-one.
    for (int i = 1; i <= amount; i++) {
      HierarchicalConfiguration subConfig = conf.configurationAt("gens/gen[" + i + "]");
      if (subConfig.containsKey("size")) {
        size = subConfig.getInt("size");
      }
      String file = subConfig.getString("fileName");
      String distribution = subConfig.getString("distribution");
      int[] generatedValues = new int[0];
      String[] generatedStringValues = new String[0];
      String returnType = "int";
      // Match the subconfiguration against predefined generation possibilities.
      switch (distribution) {
        case "binomial":
          boolean shifted = subConfig.containsKey("shift");
          if (shifted) {
            generatedValues =
                generateBinomialShifted(
                    size,
                    subConfig.getInt("shift"),
                    subConfig.getInt("trials"),
                    subConfig.getDouble("probability"));
          } else {
            generatedValues =
                generateBinomial(
                    size, subConfig.getInt("trials"), subConfig.getDouble("probability"));
          }
          break;
        case "binomialMapped":
          returnType = "String";
          int[] temp =
              generateBinomial(
                  size, subConfig.getInt("trials"), subConfig.getDouble("probability"));
          generatedStringValues =
              Utils.mapIntArrayToStrArray(MicrobenchUtils.mktsegmentValues, temp);
          break;
        case "char":
          returnType = "char";
          generatedValues = generateUniform(size, firstLetter, lastLetter);
          break;
        case "hexadecimal":
          returnType = "String";
          generatedValues = generateUniform(size, 0, 1000);
          generatedStringValues = Utils.transformIntArrayToHexArray(generatedValues);
          break;
        case "uniform":
          if (subConfig.containsKey("type")) {
            returnType = subConfig.getString("type");
          }
          generatedValues =
              generateUniform(size, subConfig.getInt("lowerbound"), subConfig.getInt("upperbound"));
          break;
        case "uniformMapped":
          if (subConfig.containsKey("type")) {
            returnType = subConfig.getString("type");
          }
          generatedStringValues = generateUniformMapped(size, subConfig.getStringArray("list"));
          break;
        case "zipf":
          int lowerbound = 0;
          if (subConfig.containsKey("lowerbound")) {
            lowerbound = subConfig.getInt("lowerbound");
          }
          generatedValues =
              generateZipf(
                  size,
                  lowerbound,
                  subConfig.getInt("numberOfElements"),
                  subConfig.getDouble("exponent"));
          break;

        case "zipfMapped":
          returnType = subConfig.getString("type");
          if (returnType.equals("String")) {
            generatedStringValues =
                generateZipfMappedtoString(
                    size, subConfig.getStringArray("list"), subConfig.getInt("exponent"));
          } else {
            lowerbound = subConfig.getInt("lowerbound");
            int upperbound = subConfig.getInt("upperbound");
            generatedValues =
                generateZipfMappedtoRandom(
                    size,
                    lowerbound,
                    upperbound,
                    upperbound - lowerbound,
                    subConfig.getInt("exponent"));
          }
          break;
        case "phoneNumber":
          if (subConfig.containsKey("onlyPrefix")) {
            generatedStringValues =
                MicrobenchUtils.generatePhoneArray(size, subConfig.getBoolean("onlyPrefix"));
          } else {
            generatedStringValues = MicrobenchUtils.generatePhoneArray(size, false);
          }
          returnType = "String";
          break;
        case "bloatMktseg":
          returnType = "String";
          String[] mktsegValues =
              Utils.bloatCardinality(
                  MicrobenchUtils.mktsegmentValues, subConfig.getInt("multiplicationFactor"));
          generatedStringValues = generateFromStringArray(size, mktsegValues);
      }

      // Check if configuration asks for correlated arrays af values. If so, generate them.
      // Afterwards store all the generated arrays in the specified file.

      //Correlation for char.
      if (returnType.equals("char")) {
        try {
          generateCorrelationChar(subConfig, generatedValues, file);
        }
        catch (Exception e){
          e.printStackTrace();
        }
        continue;
      }
      //Correlation for String.
      if (returnType.equals("String")) {
        if (!(subConfig.containsKey("correlation"))) {
          Utils.StrArrayToFile(
              generatedStringValues, storagefolder + file, subConfig.getBoolean("withIndex"));
        } else {
          generateCorrelationString(subConfig, generatedValues, file);
        }
        continue;
      }

      //Correlation for Numbers.
      ArrayList<int[]> arrays = checkAndGenerateCorrelation(generatedValues, subConfig);
      if (returnType.equals("double")) {
        storeDoubleArrays(storagefolder + file, arrays, subConfig.getBoolean("withIndex"));
      }
      if (returnType.equals("int")) {
        storeIntArrays(storagefolder + file, arrays, subConfig.getBoolean("withIndex"));
      }
    }
  }

  public void generateCorrelationChar(
      HierarchicalConfiguration subConfig, int[] generatedValues, String file) throws Exception {
    if (subConfig.containsKey("correlation")) {
      String correlationType = subConfig.getString("correlation");
      ArrayList<char[]> arrays = new ArrayList<>();
      char[] inputArrayChar = new char[generatedValues.length];
      char[] correlatedArrayChar = new char[generatedValues.length];
      if (correlationType.equals("functional dependent")) {
        String expr = subConfig.getString("expression");
        int[] correlatedArray = generateFunctionalDependency(generatedValues, expr);
        for (int i = 0; i < generatedValues.length; i++) {
          inputArrayChar[i] = (char) (generatedValues[i]);
          correlatedArrayChar[i] =
                  (char) (((correlatedArray[i] - firstLetter) % numberOfChars) + firstLetter);
        }
      } else {
        throw new Exception("This correlation type is not yet supported");
      }
      arrays.add(inputArrayChar);
      arrays.add(correlatedArrayChar);
      Utils.CharArrayArrayListToFile(
          arrays, storagefolder + file, subConfig.getBoolean("withIndex"));
    } else {
      Utils.storeIntAsChar(
          generatedValues, storagefolder + file, subConfig.getBoolean("withIndex"));
    }
  }

  public void generateCorrelationString(
      HierarchicalConfiguration subConfig, int[] generatedValues, String file) {
    ArrayList<int[]> arrays = checkAndGenerateCorrelation(generatedValues, subConfig);
    ArrayList<String[]> stringArrays = new ArrayList<>();
    for (int[] intArray : arrays) {
      stringArrays.add(Utils.transformIntArrayToHexArray(intArray));
    }
    Utils.multStringArrayToFile(
        stringArrays, storagefolder + file, subConfig.getBoolean("withIndex"));
  }

  /**
   * Generates either a correlated/functional dependent array of values or nothing depending on the
   * configuration.
   *
   * @param inputArray Array to/on which the resulting array should be correlated/functional
   *     dependent
   * @param config Configuration file specifing if a correlation/functional dependency/ nothing is
   *     asked for
   * @return
   */
  public ArrayList<int[]> checkAndGenerateCorrelation(int[] inputArray, Configuration config) {
    ArrayList<int[]> arrays = new ArrayList<>();
    arrays.add(inputArray);
    if (config.containsKey("correlation")) {
      String correlation = config.getString("correlation");
      if (correlation.equals("correlated")) {
        arrays.add(generateCorrelated(inputArray, config.getInt("CorrelationDomain")));
      } else if (correlation.equals("functional dependent")) {
        arrays.add(generateFunctionalDependency(inputArray, config.getString("expression")));
      }
    }
    return arrays;
  }


  /**
   * Stores the arrays contained in an ArrayList to a file.
   *
   * @param file
   * @param inputArrays
   * @param withIndex
   */
  public void storeIntArrays(String file, ArrayList<int[]> inputArrays, boolean withIndex) {
    Utils.multIntArrayToFile(inputArrays, file, withIndex);
  }

  /**
   * Stores the arrays contained in an ArrayList in double format to a file.
   *
   * @param file
   * @param inputArrays
   * @param withIndex
   */
  public void storeDoubleArrays(String file, ArrayList<int[]> inputArrays, boolean withIndex) {
    ArrayList<double[]> resultingDoubleArrays = new ArrayList<>();
    for (int[] inputArray : inputArrays) {
      resultingDoubleArrays.add(Utils.intArraytoDoubleArray(inputArray, 100));
    }
    Utils.multDoubleArrayToFile(resultingDoubleArrays, file, withIndex);
  }

  /**
   * Generates a String array of length "size" with Strings drawn uniformly from an inputString array.
   * @param size The length of the resulting array.
   * @param inputStrings The Strings that can be included in the resulting array.
   * @return
   */
  public String[] generateFromStringArray(int size, String[] inputStrings) {
    String[] resultingStrings = new String[size];
    for (int i = 0; i < size; i++) {
      resultingStrings[i] = inputStrings[rand.nextInt(inputStrings.length)];
    }
    return resultingStrings;
  }

  /**
   * @param quantity Number of elements in the resulting array
   * @param upperbound Upperbound for the values an element in the resulting array can take.
   *     Upperbound is inclusive.
   * @return Generates an int array following a uniform distribution
   */
  public int[] generateUniform(int quantity, int lowerbound, int upperbound) {
    UniformIntegerDistribution ud = new UniformIntegerDistribution(lowerbound, upperbound);
    return ud.sample(quantity);
  }

  /**
   * @param quantity Number of elements in the resulting array.
   * @param tokens String values which each element in the resulting array can take.
   * @return Generates an array containing uniformly distributed String values.
   */
  public String[] generateUniformMapped(int quantity, String[] tokens) {
    int upperbound = tokens.length;
    UniformIntegerDistribution ud = new UniformIntegerDistribution(0, upperbound - 1);
    String[] res = new String[quantity];
    for (int i = 0; i < quantity; i++) {
      res[i] = tokens[ud.sample()];
    }
    return res;
  }

  /**
   * @param v1 Double Array containing the values one variable.
   * @param v2 Double Array containing the values for the other variable.
   * @return the PearsonsCorrelation coefficient.
   */
  public static double correlationCoeff(double[] v1, double[] v2) {
    PearsonsCorrelation pc = new PearsonsCorrelation();
    return pc.correlation(v1, v2);
  }

  /**
   * Generates an array containing values correlated (1-to-many) to the input array. The correlated
   * values are generated by adding a different random value of
   * [-correlationDomain;correlationDomain] to each value in the original array.
   *
   * @param v original data array
   * @param correlationDomain Indicates the distance that the correlated value can be away from the
   *     original value.
   */
  public int[] generateCorrelated(int[] v, int correlationDomain) {
    int quantity = v.length;
    int[] corr = new int[quantity];
    for (int i = 0; i < quantity; i++) {
      int x = 2 * correlationDomain + 1;
      int z = rand.nextInt(x);
      corr[i] = v[i] + (z - (x / 2));
    }
    return corr;
  }

  /**
   * Generates for each element in the array a functional dependent element using the same expression.
   * @param inputarray Contains the elements.
   * @param expr Expression used to generate the functional dependent elements.
   * @return
   */
  public int[] generateFunctionalDependency(int[] inputarray, String expr) {
    int quantity = inputarray.length;
    int[] fd = new int[quantity];
    for (int i = 0; i < quantity; i++) {
      fd[i] = (int) Utils.eval(expr, inputarray[i]);
    }
    return fd;
  }

  /**
   * Generates a Zipfian distribution. The most frequent values are ordered from small to big.
   *
   * @param quantity Number of elements in the resulting array.
   * @param lowerbound The lowest value in the dataset.
   * @param numberOfElements Number of elements in the Zipf Distribution
   * @param exponent Exponent of the Zipf distribution
   * @return Generates an int array following an Zipf distribution
   */
  public int[] generateZipf(int quantity, int lowerbound, int numberOfElements, Double exponent) {
    ZipfDistribution zd = new ZipfDistribution(numberOfElements, exponent);
    int[] res = new int[quantity];
    // The sample function's lowest value it can sample is 1. We thus have to substract 1 from the
    // lowerbound to achieve the wished for lowervound in the generated dataset.
    lowerbound = lowerbound - 1;
    for (int i = 0; i < quantity; i++) {
      res[i] = lowerbound + zd.sample();
    }
    return res;
  }

  /**
   * Creates an array with values between lowerbound and upperbound following a Zipf Distribution.
   * The order of the most frequent elements is random.
   *
   * @param numberOfElements indicates the number of distinct elements in the distribution.
   * @param lowerbound indicates the inclusive lowerbound for values for each element.
   * @param upperbound indicates the exclusive upperbound for values for each element.
   */
  public int[] generateZipfMappedtoRandom(
      int quantity, int lowerbound, int upperbound, int numberOfElements, int exponent) {
    ZipfDistribution zd = new ZipfDistribution(numberOfElements, exponent);
    int[] res = new int[quantity];
    int[] map = new int[upperbound - lowerbound];
    for (int i = lowerbound; i < upperbound; i++) {
      map[i - lowerbound] = i;
    }
    map = Utils.randomizeArrayWithFixedStart(map, numberOfElements);
    for (int i = 0; i < quantity; i++) {
      res[i] = map[zd.sample() - 1];
    }
    return res;
  }

  /**
   * @param quantity Indicates the total number of elements drawn from the distribution.
   * @param elements Contains the values of the distribution. Elements at the beginning of the list
   *     will have higher cardinality.
   * @param exponent See defintion of ZipfDistribution for definition.
   * @return
   */
  public String[] generateZipfMappedtoString(int quantity, String[] elements, int exponent) {
    ZipfDistribution zd = new ZipfDistribution(elements.length, exponent);
    String[] res = new String[quantity];
    for (int i = 0; i < quantity; i++) {
      res[i] = elements[zd.sample() - 1];
    }
    return res;
  }

  /**
   * @param quantity Number of elements in the array.
   * @param trials Number of trials in the binomial distribution.
   * @param p Probability of success in the binomial distribution
   * @return Generates an int array following a binomial distribution.
   */
  public int[] generateBinomial(int quantity, int trials, double p) {
    BinomialDistribution bd = new BinomialDistribution(trials, p);
    return bd.sample(quantity);
  }

  /**
   * @param quantity Number of elements in the array.
   * @param shift Value by which the binomial is shifted in a direction.
   * @param trials Number of trials in the binomial distribution.
   * @param p Probability of success in the binomial distribution.
   * @return Generates an int array following a binomial distribution shifted into a direction
   *     specified by the shift argument.
   */
  public int[] generateBinomialShifted(int quantity, int shift, int trials, double p) {
    BinomialDistribution bd = new BinomialDistribution(trials, p);
    int[] res = new int[quantity];
    for (int i = 0; i < quantity; i++) {
      res[i] = bd.sample() + shift;
    }
    return res;
  }
}
