package framework;

import util.GenericQuery;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class Worker {
    private Connection conn;
    private ArrayList<GenericQuery> transactionQueue;
    private Random rand;
    private  int workerID=-1;
    private LatencyRecord latencies;
    private HashMap<Integer,Integer> cardinalities;

    public Worker(Connection conn, ArrayList<GenericQuery> transactionQueue, Random rand, int id){
        this.conn=conn;
        this.transactionQueue =transactionQueue;
        this.rand = rand;
        this.latencies = new LatencyRecord();
        this.cardinalities = new HashMap<>();
        this.workerID = id;
    }

    public LatencyRecord getLatencyRecord(){
        return  latencies;
    }

    public HashMap<Integer, Integer> getCardinalities() {
        return cardinalities;
    }

    public void work(String database){
        long start=-1;
        long end=-1;
        QueryPlanManager qpm = new QueryPlanManager(conn);

        for (GenericQuery query:transactionQueue){
            try{
                start = System.nanoTime();
                int countrows= query.run(conn,rand);
                cardinalities.put(query.qid, countrows);
                end = System.nanoTime();
                qpm.storeQP(query.qid, "customer", database);
            }catch(SQLException e){
                System.out.println(query.query_stmt +" produced the following SQLException:"+e);
            }
            latencies.addLatency(start,end,this.workerID, query.qid);
        }
        //cardinalities=qpm.getCardinalities();
    }
}
