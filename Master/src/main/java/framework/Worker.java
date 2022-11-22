package framework;

import util.GenericQuery;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;

public class Worker {
    private Connection conn;
    private ArrayList<GenericQuery> transactionQueue;
    private Random rand;
    private  int workerID=-1;
    private LatencyRecord latencies;

    public Worker(Connection conn, ArrayList<GenericQuery> transactionQueue, Random rand, int id){
        this.conn=conn;
        this.transactionQueue =transactionQueue;
        this.rand = rand;
        this.latencies = new LatencyRecord();
        this.workerID = id;
    }

    public LatencyRecord getLatencies(){
        return  latencies;
    }

    public void work(){
        long start=-1;
        long end=-1;

        for (GenericQuery query:transactionQueue){
            try{
                start = System.nanoTime();
                query.run(conn,rand);
                end = System.nanoTime();
            }catch(SQLException e){
                System.out.println(query.query_stmt +" produced the following SQLException:"+e);
            }
            latencies.addLatency(start,end,this.workerID, query.qid);
        }
    }
}
