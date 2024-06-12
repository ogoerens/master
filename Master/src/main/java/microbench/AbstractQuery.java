package microbench;

import org.checkerframework.checker.units.qual.A;
import util.GenericQuery;
import util.Utils;

import java.util.ArrayList;

public class AbstractQuery {
  private String abstractSqlStmt;
  private String abstractQueryName;
  private static int identifier=0;

  public AbstractQuery(String sqlStmt, String queryName) {
    this.abstractSqlStmt = sqlStmt;
    this.abstractQueryName = queryName;
  }

  /**
   * Generates a single new query replacing the placeholders in the abstract SQL query statement by
   * the values in the array.
   *
   * @param values Contains the Strings that replace the placeholders in the abstract query statement.
   * @return
   */
  public Query generateQuery(String[] values) {
    String queryStmt = String.format(this.abstractSqlStmt, (Object[]) values);
    String queryName = this.abstractQueryName + "_Values:" + Utils.join(values, "-") + "_Id:" + identifier;
    identifier ++;
    return new Query(queryStmt, queryName);
  }

  /**
   * Generates for each String Array a new Query by replacing the
   * placeholders in the abstract SQL query Statement.
   *
   * @param values Contains the Strings that replace the placeholders in the abstract query statement.
   * @return
   */
  public ArrayList<Query> generateQueries(String[][] values) {
    ArrayList<Query> queries = new ArrayList<>();
    for (String[] value : values) {
      queries.add(generateQuery(value));
    }
    return queries;
  }

  /**
   * Generates for each String element in the function argument a new Query by replacing the
   * placeholder in the abstract SQL query Statement.
   *
   * @param values Contains the Strings that replace the placeholders in the abstract query statement.
   * @return
   */
  public ArrayList<Query> generateQueries(String[] values) {
    ArrayList<Query> queries = new ArrayList<>();
    for (String value : values) {
      String queryStmt = String.format(this.abstractSqlStmt, value);
      String queryName = this.abstractQueryName + "_Value:" + value+ "_Id:" + identifier;
      identifier ++;
      queries.add(new Query(queryStmt, queryName));
    }
    return queries;
  }

  /**
   * Create a new Abstract Query with the same query statement but a new name.
   * @param newName
   * @return
   */
  public AbstractQuery getCopy(String newName){
    return new AbstractQuery(this.abstractSqlStmt, newName);
  }
}
