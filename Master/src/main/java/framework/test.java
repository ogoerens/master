package framework;

import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DisabledListDelimiterHandler;
import org.apache.commons.configuration2.tree.xpath.XPathExpressionEngine;
import util.*;

import microbench.*;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.sql.*;
import java.util.Random;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;

import org.apache.commons.configuration2.XMLConfiguration;


public class test {
    public static void main(String[] args){
        Random rand = new Random();

        Generator gTest = new Generator(rand);
        int[] rr = gTest.generateZipfMapped(10000, 10000, 100, 2);
        int[] rrCorr = gTest.generateCorrelated(rr);
        double[] w1 = Arrays.stream(rr).asDoubleStream().toArray();
        double[] w2 = Arrays.stream(rrCorr).asDoubleStream().toArray();
        double res = gTest.correlationCoeff(w1, w2);
        System.out.println(res);


        HashMap<Integer,Integer> ss= new HashMap<Integer, Integer>();
        for (int i: rr){
            if (ss.containsKey(i)){
                ss.put(i,ss.get(i)+1);
            }else{
                ss.put(i,1);
            }
        }
        for (Map.Entry<Integer,Integer> i: ss.entrySet()) {
            System.out.println(i.getKey()+" : " + i.getValue());
        }

        int[] bd= gTest.generateUniform(1500,1000);
        int[] bdCorr= gTest.generateCorrelated(bd);

        String[] tokens = {"car","velo","foot","bike","bus","plane"};
        String[] answer= gTest.generateZipfMapped(1500, microbench.utils.mktsegmentValues, 5,2 );
        for(String a:answer){
            System.out.println(a);
        }


        util.utils.StrArrayToFile(answer, "file12345.txt", true);

        ArrayList<int[]> arrs= new ArrayList<>();
        arrs.add(bd);
        arrs.add(bdCorr);


        //utils.intArrayToFile(bd, "file1234.txt");
        util.utils.multIntArrayToFile(arrs, "file1234.txt",true);


        // -- Create configuration variable
        XMLConfiguration conf= new XMLConfiguration();
        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<XMLConfiguration> builder = new FileBasedConfigurationBuilder<>(XMLConfiguration.class)
                .configure(params.xml()
                        .setFileName("benchconfig.xml")
                        .setListDelimiterHandler(new DisabledListDelimiterHandler())
                        .setExpressionEngine(new XPathExpressionEngine()));

        try {
            conf = builder.getConfiguration();
        }catch (Exception e){
            System.out.println("config building problem");
        }

        BenchConfiguration config = new BenchConfiguration(conf);
        config.init();

        try{
            Connection conn= config.makeConnection();
            if (conn != null) {
                System.out.println("Connected");
            }
            DataManager dm = new DataManager(conn);
            String[] types ={"int","int", "int"};
            //dm.updateTable("customer", "c_custkey",3,types,"'/home/olivier/Documents/MasterThesis/Master/file1234.txt'" );

            //dm.updateColumn("Customer2","c_custkey","c_mktsegment","int","char(10)","'/home/olivier/Documents/MasterThesis/Master/file12345.txt'");

            ArrayList<String> cols= new ArrayList<String>();
            cols.add("*");
            //Selection qs= new Selection("testbulk",cols," ind>5");
            ArrayList<GenericQuery> transactionqueue = new ArrayList<>();
            //GenericQuery q0 = new Selection("testbulk",cols,"ind<5");

            Q0 q0 = new Q0();
            Q0 q0a = new Q0(true);


            transactionqueue.add(q0);
            transactionqueue.add(q0a);
            //transactionqueue.add(qs);


            int numberWorkers=1;
            Worker w = new Worker(conn, transactionqueue, rand, numberWorkers);
            w.work();

            HashMap<String,ArrayList<LatencyRecord.Sample>> timesPerQuery = new HashMap<>();
            ArrayList<LatencyRecord.Sample> times = new ArrayList<LatencyRecord.Sample>();
            for (int i = 0; i < w.getLatencies().getSize(); i++) {
                String qid =w.getLatencies().getLatency(i).getQueryID();
                timesPerQuery.putIfAbsent(qid, new ArrayList<>());
                timesPerQuery.get(qid).add(w.getLatencies().getLatency(i));
                times.add(w.getLatencies().getLatency(i));
            }
            int[] time = new int[times.size()];
            for (int i = 0; i < times.size(); ++i) {
                time[i] = times.get(i).getLatencyMicroSecond();
            }

            HashMap<String,Double> averages = new HashMap<String, Double>();
            HashMap<String,int[]> ttimes = new HashMap<String, int[]>();
            for (Map.Entry<String, ArrayList<LatencyRecord.Sample>> entry: timesPerQuery.entrySet()) {
                ttimes.put(entry.getKey(), new int[entry.getValue().size()]);
                for (int i=0; i< timesPerQuery.get(entry.getKey()).size(); i++){
                    ttimes.get(entry.getKey())[i]=(timesPerQuery.get(entry.getKey()).get(i).getLatencyMicroSecond());
                }
                Statistics stats = Statistics.computeStatistics(ttimes.get(entry.getKey()));
                averages.put(entry.getKey(), stats.getAverage());
            }

            for (Map.Entry<String,Double> entry: averages.entrySet()){

                System.out.println(entry.getKey()+" : " + entry.getValue());
            }


            Statistics stats = Statistics.computeStatistics(time);
            System.out.println(stats.getAverage());
            conn.close();

            ResultWriter rw = new ResultWriter();
            String outputDirectory = "results";
            String resultsFileName = "results.csv";
            try (PrintStream ps = new PrintStream(outputDirectory + "/" + resultsFileName)) {
                rw.writeResults(stats, ps);
            } catch (FileNotFoundException e) {
                System.out.println(e);
            }
        } catch (SQLException ex) {
            System.out.println(ex);
        }
    }
}
