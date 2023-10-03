package framework;

import microbench.OldQueries;
import microbench.Queries;
import microbench.Query;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

public class QueryManager {
  private ArrayList<microbench.Query> originalQueryStore;
  private ArrayList<microbench.Query> anonymizedQueryStore;
  private ArrayList<microbench.Query> queriesForExecution;
  private ArrayList<microbench.Query> personalizedQueries;
  private int scalefactor = 1;

  public QueryManager() {
    this.originalQueryStore = new ArrayList();
    this.anonymizedQueryStore = new ArrayList<>();
    this.queriesForExecution = new ArrayList<>();
    this.personalizedQueries = new ArrayList<>();
  }

  public ArrayList<Query> getOriginalQueryStore() {
    return originalQueryStore;
  }

  public ArrayList<Query> getQueriesForExecution() {
    return queriesForExecution;
  }

  public void addQueriesForExecution(String querySetName) {
    if (querySetName.equals("anonymizedQueries")) {
      this.queriesForExecution.addAll(this.anonymizedQueryStore);
    } else {
      this.queriesForExecution.addAll(
          new ArrayList<>(Arrays.asList(returnQueryList(querySetName))));
    }
  }

  public void setAnonymizedQueryStore(ArrayList<Query> anonymizedQueryStore) {
    this.anonymizedQueryStore = anonymizedQueryStore;
  }

  public void addAnonymizedQuery(String queryName, String queryStatement) {
    this.anonymizedQueryStore.add(new Query(queryStatement, queryName, false));
  }

  public void addOriginalQueries(Query[] queries) {
    this.originalQueryStore.addAll(Arrays.asList(queries));
  }

  /**
   * Loads SQL queries from a file. Queries must be seperated by the character specified as
   * delimiter.
   *
   * @param filename THe file containing the queries.
   * @param delimiter Specifies the character that separates the queries.
   * @param queryNamePrefix The query name prefix that each query obtains.
   * @return
   * @throws FileNotFoundException
   */
  public ArrayList<Query> loadQueries(String filename, String delimiter, String queryNamePrefix)
      throws FileNotFoundException {
    File file = new File(filename);
    Scanner scanner = new Scanner(file);
    scanner.useDelimiter(delimiter);
    ArrayList<Query> queries = new ArrayList<>();
    int queryNameSuffix = 0;
    while (scanner.hasNext()) {
      queries.add(new Query(scanner.next(), queryNamePrefix + queryNameSuffix));
    }
    this.personalizedQueries.addAll(queries);
    this.queriesForExecution.addAll(queries);
    return queries;
  }

    /**
     * Returns a set of queries. Can either be a predefined set or the personalized queries.
     * Names of the different sets are: original, additional, personalized, microbenchmark, anonymization.
     * @param queryListName
     * @return
     */
    public Query[] returnQueryList(String queryListName) {
    switch (queryListName) {
      case "additional":
        return Queries.generateAddtitionalMicrobenchmarkQueries(scalefactor);
      case "personalized":
        Query[] personalizedQueriesArr = new Query[this.personalizedQueries.size()];
        return this.personalizedQueries.toArray(personalizedQueriesArr);
      case "oldOriginalQueries":
        return OldQueries.originalQueries;
      case "microbenchmark":
        // return OldQueries.microbenchmarkQueries; Original microbench
        return Queries.generateMicrobenchmarkQueries(scalefactor);
      case "original":
        return Queries.generateQueriesOnTable("customer");
      case "anonymization":
        ArrayList<Query> anonQueries = Queries.generateAnonymizationQueries();
        Query[] anonQueriesArray = new Query[anonQueries.size()];
        for (int i = 0; i < anonQueries.size(); i++) {
          anonQueriesArray[i] = anonQueries.get(i);
        }
        return anonQueriesArray;
      default:
        Query[] s = {};
        return s;
    }
  }
}
