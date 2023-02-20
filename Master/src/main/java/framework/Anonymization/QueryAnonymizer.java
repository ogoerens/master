package framework.Anonymization;

import com.google.protobuf.MapEntry;
import microbench.Query;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.deidentifier.arx.aggregates.HierarchyBuilder;
import org.deidentifier.arx.aggregates.HierarchyBuilderIntervalBased;
import org.deidentifier.arx.aggregates.HierarchyBuilderRedactionBased;
import util.StringUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryAnonymizer {
  Map<String, Integer> generalizationLevels;
  Map<String, String[][]> materializedHierarchies;
  HierarchyManager hierarchyManager;
  AnonymizationConfiguration anonConfig;

  public QueryAnonymizer(
      Map<String, Integer> generalizationLevels, HierarchyManager hierarchyManager, AnonymizationConfiguration anonConfig) {
    this.generalizationLevels = generalizationLevels;
    this.materializedHierarchies = hierarchyManager.getMaterializedHierarchies();
    this.hierarchyManager = hierarchyManager;
    this.anonConfig = anonConfig;
  }

  public String anonymize(String query) throws SqlParseException, Exception {
    return parseAndAnonymize(query);
  }

  public ArrayList<Query> anonymize(ArrayList<microbench.Query> namedQueries)
      throws SqlParseException, Exception {
    ArrayList<Query> anonymizedQueries = new ArrayList<>();
    for (microbench.Query namedQuery : namedQueries) {
      String queryWithAnonimizedLiterals = anonymize(namedQuery.query_stmt);
      String queryOnAnonyimizedTable = queryWithAnonimizedLiterals.replace("`CUSTOMER`", "\""+anonConfig.getOutputTableName() + "\"");
      String querySyntacticCorrect = queryOnAnonyimizedTable.replace("`", "\"");
      anonymizedQueries.add(new Query(querySyntacticCorrect, namedQuery.qName+"Anonymized", false));
    }
    return anonymizedQueries;
  }

  /**
   * Checks if the QueryAnonimyzer is in possession of a materialized hierarchy for a given column.
   *
   * @param column The column for which the check is executed.
   * @return
   */
  private boolean hasAnonymizedValue(String column) {
    return this.materializedHierarchies.containsKey(column);
  }

  /**
   * Returns the anonymized value for a given column and value. The anonymized value is retrieved
   * from the materialized hierarchies that the QueryAnonymizer stores.
   *
   * @param column The column that contains the value.
   * @param originalValue The value that should be anonymized.
   * @return
   * @throws Exception An execption is raised if a column for which the QueryAnonymizer is not in
   *     possession of a materialized hierarchy, is passed.
   */
  public String getAnonymizedValue(String column, String originalValue) throws Exception {
    if (!hasAnonymizedValue(column)) {
      throw new Exception(
          "Trying to get an anonymized Value for a column which has not been anonymized");
    }
    String anonyimizedValue = "";
    boolean cleanseInterval =
        hierarchyManager.getHierarchyType().get(column).equals("interval")
            && generalizationLevels.get(column) != 0;
    String[][] hierarchy = this.materializedHierarchies.get(column);
    //Linear search as hierarchies are not necessarily ordered.
    for (String[] hierarchyForValue : hierarchy) {
      String value = hierarchyForValue[0];
      String originalWithoutApostrophe = StringUtil.stripString(originalValue, "'");
      if (value.equalsIgnoreCase(originalWithoutApostrophe)) {
        // TODO check if generalization level starts at 0 or at 1!
        anonyimizedValue = hierarchyForValue[this.generalizationLevels.get(column)];
        if (cleanseInterval) {
          anonyimizedValue = ARXUtils.removeInterval(anonyimizedValue);
        }
        return anonyimizedValue;
      }
    }
    // Original Value was not found in materialized Hierarchy.
    // If we have a hierarchybuilder, we can create the anonymized value.
    if (hierarchyManager.getHierarchyStore().getIndexForColumnName(column) == 1) {
      return createAnonymizedValue(column, originalValue);
    }
    // TODO: what to do with values not in materialized hierarchies??

    return originalValue;
    // throw new Exception(          "No anonymization value found in the materialized hierarchy.
    // Hierarchy must be changed");
  }

  /**
   * Transforms a query to an anonymized query by exchanging literal values for their anonymized
   * (generalised) counterparts.
   *
   * @param query The query that isgoing to be anonymized.
   * @return
   * @throws SqlParseException
   * @throws Exception
   */
  private String parseAndAnonymize(String query) throws SqlParseException, Exception {
    SqlParser.Config sqlParserConfig = SqlParser.config().withConformance(SqlConformanceEnum.BABEL);
    SqlParser sqlParser = SqlParser.create(query, sqlParserConfig);
    SqlNode sqlNode = sqlParser.parseQuery();

    if (sqlNode instanceof SqlCall) {
      anonymizeLiterals((SqlCall) sqlNode);
    }
    // Todo change to tosqlstring ??
    return sqlNode.toString();
  }

  /**
   * @param sqlCall
   * @throws Exception
   */
  public void anonymizeLiterals(SqlCall sqlCall) throws Exception {
    SqlKind kind = sqlCall.getOperator().getKind();
    if (SqlKind.BINARY_COMPARISON.contains(kind)
        || SqlKind.BINARY_EQUALITY.contains(kind)
        || kind == SqlKind.LIKE) {
      // check if one is literal and the other identifier
      // System.out.println("CHECK FOR LITERAL in" + sqlCall.toString());
      checkForLiteralAndAnonymize(sqlCall);
      return;
    }
    List<SqlNode> operandList = sqlCall.getOperandList();
    for (SqlNode node : operandList) {
      if (node == null) {
        continue;
      }
      SqlKind sqlKind = node.getKind();
      if (node instanceof SqlNodeList) {
        continue;
      }
      if (node instanceof SqlCall) {
        anonymizeLiterals((SqlCall) node);
      }
    }
  }

  public void checkForLiteralAndAnonymize(SqlCall sqlCall) throws Exception {
    List<SqlNode> operands = sqlCall.getOperandList();
    if (operands.size() != 2) {
      throw new Exception(
          "Equals operator with more than 2 operands. This use case is currently not implemented.");
    }
    SqlNode leftNode = operands.get(0);
    SqlNode rightNode = operands.get(1);
    // Todo: check an literal creation in two functions. first check then creation.
    if (!checkForLiteralIdentifierCombination(sqlCall, 0, 1)) {
      checkForLiteralIdentifierCombination(sqlCall, 1, 0);
    }
  }

  public boolean checkForLiteralIdentifierCombination(
      SqlCall sqlCall, int operandIndex1, int operandIndex2) throws Exception {
    List<SqlNode> operands = sqlCall.getOperandList();
    SqlNode node1 = operands.get(operandIndex1);
    SqlNode node2 = operands.get(operandIndex2);
    String colName = "";
    if (node1.getKind() == SqlKind.LITERAL) {
      colName = findIdentifier(node2);
      if (!colName.equals("") && hasAnonymizedValue(colName)) {
        String anonVal = getAnonymizedValue(colName, ((SqlLiteral) node1).toValue());
        SqlLiteral literal =
            createLiteral(anonVal, node2.getParserPosition(), ((SqlLiteral) node1).getTypeName());
        sqlCall.setOperand(operandIndex1, literal);
        return true;
      }
    }
    return false;
  }

  private String createAnonymizedValue(String column, String originalValue) {
    Map<String, HierarchyBuilder> builders =
        hierarchyManager.getHierarchyStore().getHierarchyBuilders();
    if (builders.get(column) instanceof HierarchyBuilderIntervalBased) {
      // TODO change to uppercase.
      HierarchyBuilderIntervalBased intervalBased =
          (HierarchyBuilderIntervalBased) builders.get(column);
      String[] vals = {
        originalValue,
        intervalBased.getLowerRange().getBottomTopCodingFrom().toString(),
        intervalBased.getUpperRange().getBottomTopCodingFrom().toString()
      };
      intervalBased.prepare(vals);
      return ARXUtils.removeInterval(
          intervalBased.build().getHierarchy()[0][this.generalizationLevels.get(column)]);
    }
    if (builders.get(column) instanceof HierarchyBuilderRedactionBased<?>) {
      HierarchyBuilderRedactionBased redactionBased =
          (HierarchyBuilderRedactionBased) builders.get(column);
      HierarchyBuilderRedactionBased.Order order = redactionBased.getRedactionOrder();
      int size = materializedHierarchies.get(column)[0][1].length();
      int remainingLength = size - generalizationLevels.get(column);
      if (order == HierarchyBuilderRedactionBased.Order.RIGHT_TO_LEFT) {
        int k = 0;
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < originalValue.length(); i++) {
          char currentChar = originalValue.charAt(i);
          if (k == remainingLength) {
            if (currentChar == '%') {
              stringBuilder.append('%');
            }
            break;
          }
          stringBuilder.append(currentChar);
          if (currentChar != '%') {
            k++;
          }
        }
        String add = "*".repeat(generalizationLevels.get(column));
        stringBuilder.append(add);
        return stringBuilder.toString();
      } else {
        int k = 0;
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 1; i <= originalValue.length(); i++) {
          char currentChar = originalValue.charAt(originalValue.length() - i);
          if (k < remainingLength) {
            stringBuilder.append(currentChar);
            if (currentChar != '%') {
              k++;
            }
          } else {
            if (currentChar == '%') {
              stringBuilder.append("%");
            }
            break;
          }
        }
        String add = "*".repeat(generalizationLevels.get(column));
        stringBuilder.append(add);
        stringBuilder.reverse();
        return stringBuilder.toString();
      }
      // TODO. Only when like operator is used. probably needs to be put in other function.
    }
    return originalValue;
  }

  public String findIdentifier(SqlNode sqlNode) {
    if (!(sqlNode instanceof SqlCall)) {
      if (sqlNode.getKind() == SqlKind.IDENTIFIER) {
        return sqlNode.toString();
      }
    }
    List<SqlNode> operands = ((SqlCall) sqlNode).getOperandList();
    for (SqlNode node : operands) {
      if (node instanceof SqlCall) {
        findIdentifier(node);
      }
      if (node.getKind() == SqlKind.IDENTIFIER) {
        return node.toString();
      }
    }
    return "";
  }

  private SqlLiteral createLiteral(String value, SqlParserPos pos, SqlTypeName type)
      throws Exception {
    if (type == SqlTypeName.CHAR) {
      return SqlLiteral.createCharString(value, pos);
    }
    if (SqlTypeName.NUMERIC_TYPES.contains(type)) {
      if (value.matches("[0-9]+")) {
        // actual numeric value
        return SqlLiteral.createExactNumeric(value, pos);
      } else {
        // Previous numeric value has been generalized to a range. Can no longer be represented as
        // numeric value.
        return SqlLiteral.createCharString(value, pos);
      }

    } else {
      throw new Exception("Query anonimization for this SQLType has not yet been implemented");
    }
  }
}
