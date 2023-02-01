package framework.Anonymization;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import util.StringUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryAnonymizer {
  Map<String, Integer> generalizationLevels;
  Map<String, String[][]> materilaizedHierarchies;

  public QueryAnonymizer(
      Map<String, Integer> generalizationLevels, Map<String, String[][]> hierarchies) {
    this.generalizationLevels = generalizationLevels;
    this.materilaizedHierarchies = hierarchies;
  }

  public String anonymize(String query) throws SqlParseException, Exception {
    return parseAndAnonymize(query);
  }

  public void anonymize(ArrayList<String> queries) throws SqlParseException, Exception {
    for (String query : queries) {
      anonymize(query);
    }
  }

  /**
   * Checks if the QueryAnonimyzer is in possession of a materialized hierarchy for a given column.
   *
   * @param column The column for which the check is executed.
   * @return
   */
  private boolean hasAnonymizedValue(String column) {
    return this.materilaizedHierarchies.containsKey(column);
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
    String[][] hierarchy = this.materilaizedHierarchies.get(column);
    // TODO optimize search. Currently linear search...
    for (String[] hierarchyForValue : hierarchy) {
      for (String value : hierarchyForValue) {
        String originalWithoutApostrophe = StringUtil.stripString(originalValue, "'");
        if (value.equals(originalWithoutApostrophe)) {
          // TODO check if generalization level starts at 0 or at 1!
          anonyimizedValue = hierarchyForValue[this.generalizationLevels.get(column)];
        }
      }
    }
    return anonyimizedValue;
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
    //Todo: check an literal creation in two functions. first check then creation.
    if (!checkForLiteralIdentifierCombination(sqlCall, 0, 1)){
      checkForLiteralIdentifierCombination(sqlCall, 1, 0);
    }
    /*
    String colName = "";
    if (leftNode.getKind() == SqlKind.LITERAL) {
      colName = findIdentifier(rightNode);
      if (!colName.equals("") && hasAnonymizedValue(colName)) {
        String anonVal = getAnonymizedValue(colName, ((SqlLiteral) leftNode).toValue());
        SqlLiteral literal =
            createLiteral(
                anonVal, rightNode.getParserPosition(), ((SqlLiteral) rightNode).getTypeName());
        sqlCall.setOperand(0, literal);
      }
    }
    if (rightNode.getKind() == SqlKind.LITERAL) {
      colName = findIdentifier(leftNode);
      if (!colName.equals("") && hasAnonymizedValue(colName)) {
        String anonVal = getAnonymizedValue(colName, ((SqlLiteral) rightNode).toValue());
        SqlLiteral literal =
            createLiteral(
                anonVal, rightNode.getParserPosition(), ((SqlLiteral) rightNode).getTypeName());
        sqlCall.setOperand(1, literal);
      }
    }

     */
  }

  public boolean checkForLiteralIdentifierCombination(SqlCall sqlCall, int operandIndex1, int operandIndex2)
      throws Exception {
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

  public String findIdentifier(SqlNode sqlNode) {
    if (!(sqlNode instanceof SqlCall)) {
      if (sqlNode.getKind() == SqlKind.IDENTIFIER) {
        return sqlNode.toString();
      }
    }
    List<SqlNode> operands = ((SqlCall) sqlNode).getOperandList();
    for (SqlNode node : operands) {
      if (node instanceof SqlCall) {
        findIdentifier((SqlCall) node);
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
      return SqlLiteral.createExactNumeric(value, pos);
    } else {
      throw new Exception("Query anonimization for this SQLType has not yet been implemented");
    }
  }
}
