package framework.Anonymization;

import framework.Driver;
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
import java.util.ArrayList;
import java.util.Arrays;

public class AnonymizationConfiguration {
  public static final Charset charset = StandardCharsets.UTF_8;
  public static final char hierarchyValueDelimiter = ';';
  private ArrayList<PrivacyModelArguments> privacyModels;
  private ArrayList<String> insensitiveAttributes;
  private ArrayList<String> sensitiveAttributes;
  private ArrayList<String> identifyingAttributes;
  private ArrayList<String> quasiIdentifyingAttributes;
  private final ARXConfiguration.AnonymizationAlgorithm defaultAnonymizationAlgorithm =
      ARXConfiguration.AnonymizationAlgorithm.OPTIMAL;
  private String specifiedAnonymizationAlgorithm;

  private double suppressionLimit = 0;

  private ARXConfiguration config;

  public AnonymizationConfiguration(String configurationFile)
  {
    this(Driver.buildXMLConfiguration(configurationFile));
  }
  public AnonymizationConfiguration(XMLConfiguration config) {
    gatherprivacyModels(config);

    insensitiveAttributes =
        new ArrayList<>(Arrays.asList(Utils.checkAndGetArray("InsensitiveAttributes", config)));
    sensitiveAttributes =
        new ArrayList<>(Arrays.asList(Utils.checkAndGetArray("SensitiveAttributes", config)));
    identifyingAttributes =
        new ArrayList<>(Arrays.asList(Utils.checkAndGetArray("IdentifyingAttributes", config)));
    quasiIdentifyingAttributes =
        new ArrayList<>(
            Arrays.asList(Utils.checkAndGetArray("QuasiIdentifyingAttributes", config)));
    if (config.containsKey("SuppressionLimit")) {
      suppressionLimit = config.getDouble("SuppressionLimit");
    }
    this.specifiedAnonymizationAlgorithm =
        Utils.checkAndGetString("anonymizationAlgorithm", config);
  }

  public ARXConfiguration getARXConfig() {
    return config;
  }

  public ArrayList<String> getSensitiveAttributes() {
    return sensitiveAttributes;
  }

  public ArrayList<String> getInsensitiveAttributes() {
    return insensitiveAttributes;
  }

  public ArrayList<String> getQuasiIdentifyingAttributes() {
    return quasiIdentifyingAttributes;
  }

  public ArrayList<String> getIdentifyingAttributes() {
    return identifyingAttributes;
  }

  /**
   * Creates the ARXConfiguration. The ARXConfiguration is the internal configuration of the ARX
   * library that the ARXAnonymizer takes as argument when anonymizing the data. Additionally, it
   * adds the Privacy Models to the ARXConfiguration and sets several configuration variables.
   */
  public void createARXConfig() {
    this.config = ARXConfiguration.create();
    setARXAnonymizationAlgorithm(specifiedAnonymizationAlgorithm, this.config);
    this.config.setSuppressionLimit(this.suppressionLimit);
    for (PrivacyModelArguments args : this.privacyModels) {
      switch (args.privacyModel) {
        case "KAnonymity":
          config.addPrivacyModel(new KAnonymity((Integer) args.factor));
          continue;
        case "DistinctLDiversity":
          config.addPrivacyModel(new DistinctLDiversity(args.argument, (Integer) args.factor));
          continue;
        case "EntropyLDiversity":
          config.addPrivacyModel(new EntropyLDiversity(args.argument, (Double) args.factor));
          continue;
        case "OrderedDistanceTCloseness":
          config.addPrivacyModel(
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
   * Gathers the privacy models from the configuration file and adds them to the
   * PrivacyModel list.
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
   * Adds a privacy model to the Privacy Model list. A single call can add multiple
   * instance of a single privacy model.
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
   * Adds a privacy model to the Privacy Model list. A single call can add mulitple
   * instance of a single privacy model.
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
