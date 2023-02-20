package framework;

import microbench.Queries;
import microbench.Query;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class QueryManager {
    private ArrayList<microbench.Query> originalQueryStore;
    private ArrayList<microbench.Query> anonymizedQueryStore;
    private ArrayList<microbench.Query> queriesForExecution;

    public QueryManager(){
        this.originalQueryStore = new ArrayList();
        this.anonymizedQueryStore = new ArrayList<>();
        this.queriesForExecution = new ArrayList<>();
    }

    public void setOriginalQueryStore(ArrayList<Query> originalQueryStore) {
        this.originalQueryStore = originalQueryStore;
    }

    public ArrayList<Query> getAnonymizedQueryStore() {
        return anonymizedQueryStore;
    }

    public ArrayList<Query> getOriginalQueryStore() {
        return originalQueryStore;
    }

    public ArrayList<Query> getQueriesForExecution() {
        return queriesForExecution;
    }
    public void addQueriesForExecution(String querySetName) {
        if (querySetName.equals("anonymizedQueries")){
            this.queriesForExecution.addAll(this.anonymizedQueryStore);
        }else{
            this.queriesForExecution.addAll(new ArrayList<>(Arrays.asList(Queries.returnQueryList(querySetName))));
        }

    }

    public void setAnonymizedQueryStore(ArrayList<Query> anonymizedQueryStore) {
        this.anonymizedQueryStore = anonymizedQueryStore;
    }


    public void setQueriesForExecution(ArrayList<Query> queriesForExecution) {
        this.queriesForExecution = queriesForExecution;
    }

    public void addAnonymizedQuery(String queryName, String queryStatement){
        this.anonymizedQueryStore.add(new Query(queryStatement, queryName,false));
    }
    public void addOriginalQuery(String queryName, String queryStatement){
        this.originalQueryStore.add(new Query(queryStatement,queryName, false));
    }
    public void addOriginalQuery(Query query){
        this.originalQueryStore.add(query);
    }
}
