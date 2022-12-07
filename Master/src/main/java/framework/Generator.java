package framework;

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

  public Generator(Random r) {
    this.rand = r;
  }

  /**
   * Analyzes an XML configuration file and generates datasets as specified in the configuration
   * file. The generated datasets are stored in a file, where the filename is specified for each
   * generation individually.
   *
   * @param conf Contains an element indicating the number of dataset generations. Each dataset
   *     generation is then specified on its own.
   */
  public void generate(XMLConfiguration conf) {
    int amount = conf.getInt("amount");
    int size = conf.getInt("defaultSize");
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
        case "uniform":
          if (subConfig.containsKey("type")) {
            returnType = subConfig.getString("type");
          }
          generatedValues =
              generateUniform(size, subConfig.getInt("lowerbound"), subConfig.getInt("upperbound"));
          break;
        case "zipf":
          generatedValues =
              generateZipf(
                  size, subConfig.getInt("numberOfElements"), subConfig.getInt("exponent"));
          break;

        case "zipfMapped":
          returnType = subConfig.getString("type");
          if (returnType.equals("String")) {
            generatedStringValues =
                generateZipfMappedtoString(
                    size, subConfig.getStringArray("list"), subConfig.getInt("exponent"));
          } else {
            int lowerbound = subConfig.getInt("lowerbound");
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

      if (returnType.equals("String")) {
        // TODO: correlation for string fields
        Utils.StrArrayToFile(
            generatedStringValues, storagefolder + file, subConfig.getBoolean("withIndex"));
        continue;
      }
      // Check if configuration asks for correlated arrays af values. If so, generate them.
      // Afterwards store all the generated arrays in the specified file.
      ArrayList<int[]> arrays = checkAndGenerateCorrelation(generatedValues, subConfig);
      if (returnType.equals("double")) {
        storeDoubleArrays(storagefolder + file, arrays, subConfig.getBoolean("withIndex"));
      } else {
        storeIntArrays(storagefolder + file, arrays, subConfig.getBoolean("withIndex"));
      }
    }
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
        arrays.add(generateCorrelated(inputArray));
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
   *     Upperbound is inclusive
   * @return Generates an int array following a uniform distribution
   */
  public int[] generateUniform(int quantity, int lowerbound, int upperbound) {
    UniformIntegerDistribution ud = new UniformIntegerDistribution(lowerbound, upperbound);
    return ud.sample(quantity);
  }

  /**
   * @param quantity Number of elements in the resulting array.
   * @param tokens String values which each element in the resulting array can take
   * @return Generates an array containing uniformly distributed String values
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
   * @return the PearsonsCorrelation coefficient
   */
  public static double correlationCoeff(double[] v1, double[] v2) {
    PearsonsCorrelation pc = new PearsonsCorrelation();
    return pc.correlation(v1, v2);
  }

  /**
   * Genereates an array containing values correlated to the input array
   *
   * @param v original data array
   */
  public int[] generateCorrelated(int[] v) {
    int quantity = v.length;
    int[] corr = new int[quantity];
    for (int i = 0; i < quantity; i++) {
      int x = 100;
      int z = rand.nextInt(x);
      corr[i] = v[i] + (z - (x / 2));
    }
    return corr;
  }

  public int[] generateFunctionalDependency(int[] v, String expr) {
    int quantity = v.length;
    int[] fd = new int[quantity];
    for (int i = 0; i < quantity; i++) {
      fd[i] = (int) Utils.eval(expr, v[i]);
    }
    return fd;
  }

  public int[] generatePoisson(int quantity, double p) {
    PoissonDistribution pd = new PoissonDistribution(p);
    return pd.sample(quantity);
  }

  /**
   * @param quantity Number of elements in the resulting array.
   * @param numberOfElements Number of elements in the Zipf Distribution
   * @param exponent Exponent of the Zipf distribution
   * @return Generates an int array following an Zipf distribution
   */
  public int[] generateZipf(int quantity, int numberOfElements, int exponent) {
    ZipfDistribution zd = new ZipfDistribution(numberOfElements, exponent);
    int[] res = new int[quantity];
    for (int i = 0; i < quantity; i++) {
      res[i] = zd.sample();
    }
    return res;
  }

  /**
   * Creates an array with values between lowerbound and upperbound following a ZipfDistribution.
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
   * @param quantity indicates the total number of elements drawn from the distribution
   * @param elements contains the values of the distribution. Elements at the beginning of the list
   *     will have higher cardinality.
   * @param exponent ,see defintion of ZipfDistribution for definition
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

  public int[] generateZipfMappedtoInt(int quantity, int[] elements, int exponent) {
    ZipfDistribution zd = new ZipfDistribution(elements.length, exponent);
    int[] res = new int[quantity];
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
