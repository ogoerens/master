package framework.Anonymization;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.deidentifier.arx.ARXConfiguration;
import org.deidentifier.arx.criteria.DistinctLDiversity;
import org.deidentifier.arx.criteria.EntropyLDiversity;
import org.deidentifier.arx.criteria.KAnonymity;
import org.deidentifier.arx.criteria.OrderedDistanceTCloseness;
import util.Utils;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.UnaryOperator;

public class AnonymizationConfiguration {
  public static final Charset charset = StandardCharsets.UTF_8;
  public static final char hierarchyValueDelimiter = ';';
  private String anonymizationStrategy = "";
  private String dataStorageMethod;
  private String dataTableName;
  private String dataFileName;
  private String domainFileLocation;
  private String outputFileName;
  private String outputTableName;
  private String[] querysetNames;
  private boolean queryAnonimization = false;
  private ArrayList<PrivacyModelArguments> privacyModels;
  private ArrayList<String> insensitiveAttributes;
  private ArrayList<String> sensitiveAttributes;
  private ArrayList<String> identifyingAttributes;
  private ArrayList<String> quasiIdentifyingAttributes;

  private String specifiedAnonymizationAlgorithm;
  private HashMap<String, Integer> maximalGeneralizationLevels;
  private String hashingFunction;
  private String hierarchyFile;
  private String[] hashingColumns;
  private String[] columnsForSynth;
  private String[] remainingColumns;

  private double suppressionLimit = 1000;

  private ARXConfiguration arxConfig;

  public AnonymizationConfiguration(String configurationFile) {
    this(Utils.buildXMLConfiguration(configurationFile));
  }

  public AnonymizationConfiguration(XMLConfiguration config) throws RuntimeException {
    this.anonymizationStrategy = config.getString("AnonymizationStrategy");
    if (this.anonymizationStrategy.equalsIgnoreCase("hash")) {
      this.anonymizationStrategy = "Hash";
    }

    HierarchicalConfiguration dataSubconfig = config.configurationAt("Data");
    this.dataStorageMethod = dataSubconfig.getString("StorageMethod");
    String storageName = dataSubconfig.getString("StorageName");
    if (this.dataStorageMethod.equalsIgnoreCase("file")) {
      this.dataFileName = storageName;
    } else {
      this.dataTableName = storageName.toUpperCase();
    }

    if (dataSubconfig.containsKey("outputTableName")) {
      this.outputTableName = dataSubconfig.getString("outputTableName").toUpperCase();
    } else {
      throw new RuntimeException(
          "No output table name specified in anonymization configuration. Please specifiy output table name using the tag: outputTableName.");
    }
    if (dataSubconfig.containsKey("querysetName")) {
      this.queryAnonimization = true;
      this.querysetNames = dataSubconfig.getStringArray("querysetName");
    }

    // All configuration arguments for the hashing operation are retrieved.
    if (this.anonymizationStrategy.equalsIgnoreCase("hash")) {
      HierarchicalConfiguration hashSubconfig = config.configurationAt("Hash");
      this.hashingFunction = hashSubconfig.getString("HashingFunction");
      String[] hashingColumnsRandom = hashSubconfig.getStringArray("Columns");
      this.hashingColumns = new String[hashingColumnsRandom.length];
      for (int i = 0; i < hashingColumnsRandom.length; i++) {
        this.hashingColumns[i] = hashingColumnsRandom[i].toUpperCase();
      }
      return;
    }
    // All configuration arguments for the hashing operation are retrieved.
    if (this.anonymizationStrategy.equalsIgnoreCase("Synth")) {
      HierarchicalConfiguration synthSubconfig = config.configurationAt("Synth");
      this.domainFileLocation = synthSubconfig.getString("domainLocation");
      this.columnsForSynth = synthSubconfig.getStringArray("columnsForSynth");
      this.remainingColumns = synthSubconfig.getStringArray("remainingCols");
    }

    // Retrieve the remaining arguments for the other anonymization techniques.
    if (dataSubconfig.containsKey("outputFileName")) {
      this.outputFileName = dataSubconfig.getString("outputFileName");
    } else {
      throw new RuntimeException(
          "No output file name specified in anonymization configuration. Please specifiy output file name using the tag: outputFileName.");
    }

    this.hierarchyFile = config.getString("hierarchyFile");

    gatherMaximalGeneralizationLevels(config);

    gatherprivacyModels(config);

    insensitiveAttributes =
        new ArrayList<>(
            transformStringArrayToList(
                Utils.checkAndGetArray("InsensitiveAttributes", config), String::toUpperCase));
    sensitiveAttributes =
        new ArrayList<>(
            transformStringArrayToList(
                Utils.checkAndGetArray("SensitiveAttributes", config), String::toUpperCase));
    identifyingAttributes =
        new ArrayList<>(
            transformStringArrayToList(
                Utils.checkAndGetArray("IdentifyingAttributes", config), String::toUpperCase));
    quasiIdentifyingAttributes =
        new ArrayList<>(
            transformStringArrayToList(
                Utils.checkAndGetArray("QuasiIdentifyingAttributes", config), String::toUpperCase));
    if (config.containsKey("SuppressionLimit")) {
      suppressionLimit = config.getDouble("SuppressionLimit");
    }
    this.specifiedAnonymizationAlgorithm =
        Utils.checkAndGetString("anonymizationAlgorithm", config);

    createARXConfig();
  }

  public String getAnonymizationStrategy() {
    return anonymizationStrategy;
  }

  public ARXConfiguration getARXConfig() {
    return arxConfig;
  }

  public String[] getColumnsForSynth() {
    return columnsForSynth;
  }

  public String getDataTableName() {
    return dataTableName;
  }

  public String getDataStorageMethod() {
    return dataStorageMethod;
  }

  public String getDomainFileLocation() {
    return domainFileLocation;
  }

  public String getHashingFunction() {
    return hashingFunction;
  }

  public String[] getHashingColumns() {
    return hashingColumns;
  }

  public String getHierarchyFile() throws Exception {
    if (hierarchyFile == null) {
      throw new Exception(
          "The field hierarchyFile has not been set in the anonymization configuration. ");
    }
    return hierarchyFile;
  }

  public ArrayList<String> getIdentifyingAttributes() {
    return identifyingAttributes;
  }

  public ArrayList<String> getInsensitiveAttributes() {
    return insensitiveAttributes;
  }

  public String[] getRemainingColumns() {
    return remainingColumns;
  }

  public ArrayList<String> getSensitiveAttributes() {
    return sensitiveAttributes;
  }

  public ArrayList<String> getQuasiIdentifyingAttributes() {
    return quasiIdentifyingAttributes;
  }

  public Boolean getQueryAnonimization() {
    return this.queryAnonimization;
  }

  public String[] getQuerysetNames() {
    return querysetNames;
  }

  public String getOutputFileName() {
    return outputFileName;
  }

  public String getOutputTableName() {
    return outputTableName;
  }

  public HashMap<String, Integer> getMaximalGeneralizationLevels() {
    return maximalGeneralizationLevels;
  }

  /**
   * Creates the ARXConfiguration. The ARXConfiguration is the internal configuration of the ARX
   * library that the ARXAnonymizer takes as argument when anonymizing the data. Additionally, it
   * adds the Privacy Models to the ARXConfiguration and sets several configuration variables.
   */
  public void createARXConfig() {
    this.arxConfig = ARXConfiguration.create();
    setARXAnonymizationAlgorithm(specifiedAnonymizationAlgorithm, this.arxConfig);
    this.arxConfig.setSuppressionLimit(this.suppressionLimit);
    this.arxConfig.setHeuristicSearchThreshold(100000000);
    for (PrivacyModelArguments args : this.privacyModels) {
      switch (args.privacyModel) {
        case "KAnonymity":
          arxConfig.addPrivacyModel(new KAnonymity((Integer) args.factor));
          continue;
        case "DistinctLDiversity":
          arxConfig.addPrivacyModel(new DistinctLDiversity(args.argument, (Integer) args.factor));
          continue;
        case "EntropyLDiversity":
          arxConfig.addPrivacyModel(new EntropyLDiversity(args.argument, (Double) args.factor));
          continue;
        case "OrderedDistanceTCloseness":
          arxConfig.addPrivacyModel(
              new OrderedDistanceTCloseness(args.argument, (Double) args.factor));
      }
    }
  }

  /**
   * Sets the anonymization algorithm in the ARXConfiguration to the algorithm indicated by the
   * String value.
   *
   * @param specifiedAnonymizationAlgorithm Indicates which algorithm should be used.
   * @param arxConfiguration Specifies the arxConfiguration in which the anonymizing algorithm is
   *     set.
   */
  private void setARXAnonymizationAlgorithm(
      String specifiedAnonymizationAlgorithm, ARXConfiguration arxConfiguration) {
    switch (specifiedAnonymizationAlgorithm) {
      case "OPTIMAL":
        break;
      case "BEST_EFFORT_BINARY":
        arxConfiguration.setAlgorithm(ARXConfiguration.AnonymizationAlgorithm.BEST_EFFORT_BINARY);
        break;
      case "BEST_EFFORT_BOTTOM_UP":
        arxConfiguration.setAlgorithm(
            ARXConfiguration.AnonymizationAlgorithm.BEST_EFFORT_BOTTOM_UP);
        break;
      case "BEST_EFFORT_TOP_DOWN":
        arxConfiguration.setAlgorithm(ARXConfiguration.AnonymizationAlgorithm.BEST_EFFORT_TOP_DOWN);
        break;
      case "BEST_EFFORT_GENETIC":
        arxConfiguration.setAlgorithm(ARXConfiguration.AnonymizationAlgorithm.BEST_EFFORT_GENETIC);
        break;
      default:
        System.out.println(
            "No implemented anonymization algorithm specified. Using default algorithm (OPTIMAL).");
    }
  }

  /**
   * Retrieve the maximal generalizationLevels.
   *
   * @param config
   */
  private void gatherMaximalGeneralizationLevels(XMLConfiguration config) {
    this.maximalGeneralizationLevels = new HashMap<>();
    HierarchicalConfiguration generalizationLevelsConfig =
        config.configurationAt("GeneralizationLevels");
    int instanceCount = generalizationLevelsConfig.getInt("Count");
    for (int i = 1; i <= instanceCount; i++) {
      HierarchicalConfiguration instance =
          generalizationLevelsConfig.configurationAt("Instance[" + i + "]");
      maximalGeneralizationLevels.put(
          instance.getString("Column").toUpperCase(), instance.getInt("Level"));
    }
  }

  /**
   * Gathers the privacy models from the configuration file and adds them to the PrivacyModel list.
   *
   * @param config XMLConfiguration that contains the anonymization configuration.
   */
  private void gatherprivacyModels(XMLConfiguration config) {
    String[] anonymizationCriteriasArray = config.getStringArray("anonymizationCriterias");
    this.privacyModels = new ArrayList<>();
    for (String criteria : anonymizationCriteriasArray) {
      retrievePrivacyModel(criteria, config);
    }
  }

  /**
   * Retrieves a specific privacy model from the configuration file. For each privacy model all
   * instances are retrieved during one invocation of the function. Each instance of a privacy model
   * comes with a factor and possibly with an attribute on which it is applied. These factors and
   * attributes are also retrieved.
   *
   * @param privacyModel The privacy model that should be retrieved.
   * @param config The XMLConfiguration representing the configuration file.
   */
  private void retrievePrivacyModel(String privacyModel, XMLConfiguration config) {
    // Retrieve for k-anonymity.
    HierarchicalConfiguration subconfig = config.configurationAt(privacyModel);
    switch (privacyModel) {
      case "KAnonymity":
        int factor = subconfig.getInt("factors");
        this.privacyModels.add(new PrivacyModelArguments<Integer>(privacyModel, factor, ""));
        break;
      case "DistinctLDiversity":
        Object factorsObject = subconfig.getArray(Integer.class, "factors");
        if (factorsObject == null) {
          throwExceptionMissingFactors(privacyModel);
        }
        Integer[] factors = (Integer[]) factorsObject;
        String[] attributes = Utils.checkAndGetArray("arguments", subconfig);
        addPrivacyModel(privacyModel, factors, attributes);
        break;
      case "EntropyLDiversity":
      case "OrderedDistanceTCloseness":
        factorsObject = subconfig.getArray(Double.class, "factors");
        if (factorsObject == null) {
          throwExceptionMissingFactors(privacyModel);
        }
        Double[] factorsD = (Double[]) factorsObject;
        attributes = Utils.checkAndGetArray("arguments", subconfig);
        addPrivacyModel(privacyModel, factorsD, attributes);
        break;
      default:
        throw new RuntimeException(
            "The specified privacy model (" + privacyModel + ") has not been implemented yet");
    }
  }

  /**
   * Adds a privacy model to the Privacy Model list. A single call can add multiple instance of a
   * single privacy model.
   *
   * @param criteria The privacy Model that gets added.
   * @param factors Contains the factors for each instance of the privacy model.
   * @param attributes Contains the attributes on which each instance of the privacy model is
   *     applied.
   */
  private void addPrivacyModel(String criteria, Integer[] factors, String[] attributes) {
    if (factors.length == attributes.length) {
      for (int i = 0; i < factors.length; i++) {
        this.privacyModels.add(
            new PrivacyModelArguments<Integer>(criteria, factors[i], attributes[i]));
      }
    } else {
      throwExceptionNonmatchingLengths(criteria);
    }
  }

  /**
   * Adds a privacy model to the Privacy Model list. A single call can add mulitple instance of a
   * single privacy model.
   *
   * @param criteria The privacy Model that gets added.
   * @param factors Contains the factors for each instance of the privacy model.
   * @param attributes Contains the attributes on which each instance of the privacy model is
   *     applied.
   */
  private void addPrivacyModel(String criteria, Double[] factors, String[] attributes) {
    if (factors.length == attributes.length) {
      for (int i = 0; i < factors.length; i++) {
        this.privacyModels.add(
            new PrivacyModelArguments<Double>(criteria, factors[i], attributes[i]));
      }
    } else {
      throwExceptionNonmatchingLengths(criteria);
    }
  }

  private void throwExceptionNonmatchingLengths(String criteria) throws RuntimeException {
    String errMsg =
        String.format(
            "Number of factors and arguments in the configuration file does match for the $s privacy model",
            criteria);
    throw new RuntimeException(errMsg);
  }

  private void throwExceptionMissingFactors(String criteria) throws RuntimeException {
    String errMsg =
        String.format(
            "Missing the factors for the %s privacy model. Please check that the factors are specified in the anonymization configuration file using the tag <factors>.",
            criteria);
    throw new RuntimeException(errMsg);
  }

  private List transformStringArrayToList(String[] strArray, UnaryOperator<String> unaryOperator) {
    List list = Arrays.asList(strArray);
    list.replaceAll(unaryOperator);
    return list;
  }

  /**
   * Class that represents an instance of a privacy model. It contains the name of the privacy
   * model, its factor and the attribute on which the privacy model is applied. If the privacy model
   * is not applied to a specific attribute, the attribute value is an empty String.
   *
   * @param <E> The datatype of the factor. It can be either an Integer or a Double depending on the
   *     privacy model.
   */
  private class PrivacyModelArguments<E> {
    String privacyModel;
    E factor;
    String argument;

    PrivacyModelArguments(String privacyModel, E factor, String argument) {
      this.privacyModel = privacyModel;
      this.factor = factor;
      this.argument = argument;
    }
  }
}
