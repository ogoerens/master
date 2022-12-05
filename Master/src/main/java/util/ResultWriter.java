package util;


import framework.Statistics;
import java.io.PrintStream;

public class ResultWriter {

  public static final double MILLISECONDS_FACTOR = 1e3;

  public void writeResults(Statistics s, PrintStream out) {
    String[] header = {
      "Average Latency (millisecond)",
      "Minimum Latency (millisecond)",
      "25th Percentile Latency (millisecond)",
      "Median Latency (millisecond)",
      "75th Percentile Latency (millisecond)",
      "90th Percentile Latency (millisecond)",
      "95th Percentile Latency (millisecond)",
      "99th Percentile Latency (millisecond)",
      "Maximum Latency (millisecond)",
      "tp (req/s) scaled"
    };
    out.println(StringUtil.join(",", header));
    int i = 0;

    out.printf(
        "%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f\n",
        s.getAverage() / MILLISECONDS_FACTOR,
        s.getMinimum() / MILLISECONDS_FACTOR,
        s.get25thPercentile() / MILLISECONDS_FACTOR,
        s.getMedian() / MILLISECONDS_FACTOR,
        s.get75thPercentile() / MILLISECONDS_FACTOR,
        s.get90thPercentile() / MILLISECONDS_FACTOR,
        s.get95thPercentile() / MILLISECONDS_FACTOR,
        s.get99thPercentile() / MILLISECONDS_FACTOR,
        s.getMaximum() / MILLISECONDS_FACTOR,
        MILLISECONDS_FACTOR / s.getAverage());
  }
}
