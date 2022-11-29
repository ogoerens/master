package framework;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
public class LatencyRecord {

    private ArrayList<Sample> values = new ArrayList<Sample>();
    private long startNanosecond;
    private long endNanosecond;

    public void addLatency(long start, long end, int workerID, int qID ){
        values.add(new Sample(start, end, workerID,qID));
    }
    public void addLatency(Sample sample ){
        values.add(sample);
    }

    public int getSize(){
        return values.size();
    }

    public Sample getLatency(int i){
        return values.get(i);
    }


    public HashMap<Integer,LatencyRecord> attachToQuery(ArrayList<Integer> qidToQName){
        HashMap<Integer,LatencyRecord> timesPerQuery = new HashMap<>();
        for (int i = 0; i < values.size(); i++) {
            int qid =qidToQName.get(values.get(i).getQueryID());
            timesPerQuery.putIfAbsent(qid, new LatencyRecord());
            timesPerQuery.get(qid).addLatency(values.get(i));
        }
        return timesPerQuery;
    }

    public int[] getLatenciesAsArray(){
        int[] times = new int[values.size()];
        for (int i = 0; i < values.size(); ++i) {
            times[i] = values.get(i).getLatencyMicroSecond();
        }
        return times;
    }


    public class Sample{
        private long startNanosecond;
        private int latency;
        private int workerId;
        private int queryID;

        Sample( long start, long end, int workerID, int queryID){
            this.startNanosecond=start;
            this.latency= (int) ((end-start +500)/1000);
            this.workerId=workerID;
            this.queryID=queryID;
        }

        public int getLatencyMicroSecond() {
            return latency;
        }
        public int getQueryID(){
            return queryID;
        }
    }
}
