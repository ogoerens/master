package framework;

import microbench.Query;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;

public class QueryManager {
    private HashMap<String, String> originalQueries;
    private HashMap<String, String> anonymizedQueries;
    private ArrayList<microbench.Query> originalQueryStore;
    private ArrayList<microbench.Query> anonymizedQueryStore;

    public QueryManager(){
        this.originalQueries = new HashMap<>();
        this.anonymizedQueries = new HashMap<>();
        this.originalQueryStore = new ArrayList();
        this.anonymizedQueryStore = new ArrayList<>();
    }

    public HashMap<String, String> getOriginalQueries() {
        return originalQueries;
    }

    public HashMap<String, String> getAnonymizedQueries() {
        return anonymizedQueries;
    }

    public ArrayList<Query> getAnonymizedQueryStore() {
        return anonymizedQueryStore;
    }

    public ArrayList<Query> getOriginalQueryStore() {
        return originalQueryStore;
    }

    public void setOriginalQueries(HashMap<String, String> originalQueries) {
        this.originalQueries = originalQueries;
    }

    public void setAnonymizedQueries(HashMap<String, String> anonymizedQueries) {
        this.anonymizedQueries = anonymizedQueries;
    }

    public void setAnonymizedQueryStore(ArrayList<Query> anonymizedQueryStore) {
        this.anonymizedQueryStore = anonymizedQueryStore;
    }

    public void addAnonymizedQuery(String queryName, String queryStatement){
        this.anonymizedQueries.put(queryName,queryStatement);
        this.anonymizedQueryStore.add(new Query(queryStatement, queryName,false));
    }
    public void addOriginalQuery(String queryName, String queryStatement){
        this.originalQueries.put(queryName,queryStatement);
        this.originalQueryStore.add(new Query(queryStatement,queryName, false));
    }
}
