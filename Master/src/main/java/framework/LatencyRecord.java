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

  /**
   * Groups all Latencies per queryname that is associated with the query.
   *
   * @return Linked HashMap that contains the latencies per queryname.
   */
  public LinkedHashMap<String, LatencyRecord> groupQueriesPerName() {
    LinkedHashMap<String, LatencyRecord> timesPerQuery = new LinkedHashMap<>();
    for (int i = 0; i < this.values.size(); i++) {
      String qName = values.get(i).getQueryName();
      timesPerQuery.putIfAbsent(qName, new LatencyRecord());
      timesPerQuery.get(qName).addLatency(values.get(i));
    }
    return timesPerQuery;
  }

  /**
   * Groups all Latencies per query Number and distribution that is associated with the query. Query
   * number and Distribution are extracted from queryname.
   *
   * @return
   */
  public LinkedHashMap<String, LatencyRecord> groupQueriesPerDistribution() {
    LinkedHashMap<String, LatencyRecord> timesPerQuery = new LinkedHashMap<>();
    for (int i = 0; i < this.values.size(); i++) {
      String qName = values.get(i).getQueryName();
      String distribution = extractDistribution(qName);
      if (!distribution.equals("-1")) {
        int index = qName.indexOf("_");
        timesPerQuery.putIfAbsent(
            qName.substring(0, index) + "_" + distribution, new LatencyRecord());
        timesPerQuery.get(qName.substring(0, index) + "_" + distribution).addLatency(values.get(i));
      }
    }

    return timesPerQuery;
  }

  /**
   * Extracts the distribution out of the String. Distributions can be followed by an integer if
   * there are multiple datasets with this distribution. TODO: If QueryName implementation gets
   * updated, extract by position not match.
   *
   * @param str
   * @return
   */
  public static String extractDistribution(String str) {
    String anon = "";
    if (str.toLowerCase().contains("anonymized")){
      anon= "anon";
    }
    if (str.toLowerCase().contains("binomial2")) {
      return "binomial2"+anon;
    }
    if (str.toLowerCase().contains("binomial3")) {
      return "binomial3"+anon;
    }
    if (str.toLowerCase().contains("binomial")) {
      return "binomial"+anon;
    }

    if (str.toLowerCase().contains("zipf1")) {
      return "zipf1"+anon;
    }
    if (str.toLowerCase().contains("zipf2")) {
      return "zipf2"+anon;
    }
    if (str.toLowerCase().contains("zipf3")) {
      return "zipf3"+anon;
    }
    if (str.toLowerCase().contains("zipf")) {
      return "zipf"+anon;
    }
    if (str.toLowerCase().contains("uncorrelated")) {
      return "uncorrelated"+anon;
    }
    if (str.toLowerCase().contains("fd")) {
      return "fd"+anon;
    }
    if (str.toLowerCase().contains("biggerdomain")) {
      return "biggerdomain"+anon;
    }
    if (str.toLowerCase().contains("lowerdomain")) {
      return "lowerdomain"+anon;
    }
    if (str.toLowerCase().contains("customer-")) {
      return "uniform"+anon;
    }
    return "-1"+anon;
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
