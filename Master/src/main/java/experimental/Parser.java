package experimental;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import org.apache.calcite.sql.validate.SqlConformanceEnum;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Parser {
  Map<String, Integer> literalPos;

  public void parse(String query) throws SqlParseException, Exception {

    SqlParser.Config sqlParserConfig = SqlParser.config().withConformance(SqlConformanceEnum.BABEL);
    SqlParser sqlParser = SqlParser.create(query, sqlParserConfig);
    SqlNode sqlNode = sqlParser.parseQuery();

    if (sqlNode instanceof SqlCall) {
      checkforOperands((SqlCall) sqlNode);
    }
  }

  public void checkforOperands(SqlCall sqlCall) throws Exception {
    SqlKind kind = sqlCall.getOperator().getKind();
    if (SqlKind.BINARY_COMPARISON.contains(kind)
        || SqlKind.BINARY_EQUALITY.contains(kind)
        || kind == SqlKind.LIKE) {
      // check if one is literal and the other identifier
      System.out.println("CHECK FOR LITERAL in" + sqlCall.toString());
      checkForLiteral(sqlCall);
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
        checkforOperands((SqlCall) node);
      }
    }
  }

  public void checkForLiteral(SqlCall sqlCall) throws Exception {
    List<SqlNode> operands = sqlCall.getOperandList();
    if (operands.size() != 2) {
      throw new Exception(
          "Equals operator with more than 2 operands. This use case is currently not implemented.");
    }
    SqlNode leftNode = operands.get(0);
    SqlNode rightNode = operands.get(1);
    String colName = "";
    if (leftNode.getKind() == SqlKind.LITERAL) {
      colName = checkForIdentifier(rightNode);
    }
    if (rightNode.getKind() == SqlKind.LITERAL) {
      colName = checkForIdentifier(leftNode);

    }
    System.out.println("this is colName:" + colName);
  }

  public String checkForIdentifier(SqlNode sqlNode) {
    if (!(sqlNode instanceof SqlCall)) {
      if (sqlNode.getKind() == SqlKind.IDENTIFIER) {
        return sqlNode.toString();
      }
    }
    List<SqlNode> operands = ((SqlCall) sqlNode).getOperandList();
    for (SqlNode node : operands) {
      if (node instanceof SqlCall) {
        checkForIdentifier((SqlCall) node);
      }
      if (node.getKind() == SqlKind.IDENTIFIER) {
        return node.toString();
      }
    }
    return "";
  }
}
