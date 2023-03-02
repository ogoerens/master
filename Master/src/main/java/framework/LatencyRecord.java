package framework;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;


public class LatencyRecord {

  private ArrayList<Sample> values = new ArrayList<Sample>();
  private long startNanosecond;
  private long endNanosecond;

  public void addLatency(long start, long end, int workerID, String qName) {
    values.add(new Sample(start, end, workerID, qName));
  }

  public void addLatency(Sample sample) {
    values.add(sample);
  }

  public int getSize() {
    return values.size();
  }

  public Sample getLatency(int i) {
    return values.get(i);
  }

  public LinkedHashMap<String, LatencyRecord> groupQueriesPerName() {
    LinkedHashMap<String, LatencyRecord> timesPerQuery = new LinkedHashMap<>();
    for (int i = 0; i < this.values.size(); i++) {
      String qName = values.get(i).getQueryName();
      timesPerQuery.putIfAbsent(qName, new LatencyRecord());
      timesPerQuery.get(qName).addLatency(values.get(i));
    }
    return timesPerQuery;
  }

  public int[] getLatenciesAsArray() {
    int[] times = new int[values.size()];
    for (int i = 0; i < values.size(); ++i) {
      times[i] = values.get(i).getLatencyMicroSecond();
    }
    return times;
  }


  public class Sample {
    private long startNanosecond;
    private int latency;
    private int workerId;
    private String queryName;

    Sample(long start, long end, int workerID, String queryName) {
      this.startNanosecond = start;
      this.latency = (int) ((end - start + 500) / 1000);
      this.workerId = workerID;
      this.queryName = queryName;
    }

    public int getLatencyMicroSecond() {
      return latency;
    }

    public String getQueryName() {
      return queryName;
    }
  }
}
