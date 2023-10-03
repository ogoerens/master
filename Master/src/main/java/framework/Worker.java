package framework;

import microbench.Query;
import util.GenericQuery;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class Worker {
  private Connection conn;
  private ArrayList<Driver.QueryBool> transactionQueue;
  private Random rand;
  private int workerID = -1;
  private LatencyRecord latencies;
  private HashMap<String, Integer> cardinalities;

  public Worker(Connection conn, ArrayList<Driver.QueryBool> transactionQueue, Random rand, int id) {
    this.conn = conn;
    this.transactionQueue = transactionQueue;
    this.rand = rand;
    this.latencies = new LatencyRecord();
    this.cardinalities = new HashMap<>();
    this.workerID = id;
  }

  /**
   * Returns the latency record containing all the latency sample of queries that the worker has executed.
   */
  public LatencyRecord getLatencyRecord() {
    return latencies;
  }

  /**
   * Returns the cardinalities of all the queries that the worker has executed. For each query name, a single cardinality value is stored.
   */
  public HashMap<String, Integer> getCardinalities() {
    return cardinalities;
  }

  /**
   * Iterates through its transaction queue and executes query after query. For each query, it
   * records the execution time and adds it into the latencies LatencyRecord. Additionally, it stores
   * the number of returned rows for each query.
   *
   * @param database Database in which the queries are run.
   */
  public void work(String database) {
    long start = -1;
    long end = -1;
    QueryPlanManager qpm = new QueryPlanManager(conn);

    for (Driver.QueryBool queryBool : transactionQueue) {
      GenericQuery query = queryBool.query;
      try {
        start = System.nanoTime();
        int countrows = query.run(conn, rand);
        if (!cardinalities.containsKey(query.qName)){
          cardinalities.put(query.qName, countrows);
        }
        end = System.nanoTime();
        qpm.storeQP(query.qName, "customer", database);
        //Clears the data cache in MSSQL Server.
        //Query q = new Query("CHECKPOINT; DBCC DROPCLEANBUFFERS; DBCC FREEPROCCACHE",-1);
        //Removes all cached execution plans.
        Query q = new Query("DBCC FREEPROCCACHE",-1);
        q.update(conn);
      } catch (SQLException e) {
        System.out.println(query.query_stmt + " produced the following SQLException:" + e);
      }
      if (queryBool.time){
        latencies.addLatency(start, end, this.workerID, query.qName);
      }

    }
  }
}
