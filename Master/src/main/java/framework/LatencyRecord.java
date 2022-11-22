package framework;

import java.util.ArrayList;

public class LatencyRecord {

    private ArrayList<Sample> values = new ArrayList<Sample>();
    private long startNanosecond;
    private long endNanosecond;

    public void addLatency(long start, long end, int workerID, String qID ){
        values.add(new Sample(start, end, workerID,qID));
    }

    public int getSize(){
        return values.size();
    }

    public Sample getLatency(int i){
        return values.get(i);
    }

    public class Sample{
        private long startNanosecond;
        private int latency;
        private int workerId;
        private String queryID;

        Sample( long start, long end, int workerID, String queryID){
            this.startNanosecond=start;
            this.latency= (int) ((end-start +500)/1000);
            this.workerId=workerID;
            this.queryID=queryID;
        }

        public int getLatencyMicroSecond() {
            return latency;
        }
        public String getQueryID(){
            return queryID;
        }
    }
}
