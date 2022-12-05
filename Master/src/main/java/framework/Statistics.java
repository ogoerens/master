package framework;

import java.util.Arrays;

public class Statistics {

  private static final double[] PERCENTILES = {0.0, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1.0};

  private static final int MINIMUM = 0;
  private static final int PERCENTILE_25TH = 1;
  private static final int MEDIAN = 2;
  private static final int PERCENTILE_75TH = 3;
  private static final int PERCENTILE_90TH = 4;
  private static final int PERCENTILE_95TH = 5;
  private static final int PERCENTILE_99TH = 6;
  private static final int MAXIMUM = 7;

  private final int count;
  private final long[] percentiles;
  private final double average;
  private final double standardDeviation;

  public Statistics(int count, long[] percentiles, double average, double standardDeviation) {
    this.count = count;
    this.percentiles = Arrays.copyOfRange(percentiles, 0, PERCENTILES.length);
    this.average = average;
    this.standardDeviation = standardDeviation;
  }

  public int getCount() {
    return count;
  }

  public double getAverage() {
    return average;
  }

  public double getStandardDeviation() {
    return standardDeviation;
  }

  public double getMinimum() {
    return percentiles[MINIMUM];
  }

  public double get25thPercentile() {
    return percentiles[PERCENTILE_25TH];
  }

  public double getMedian() {
    return percentiles[MEDIAN];
  }

  public double get75thPercentile() {
    return percentiles[PERCENTILE_75TH];
  }

  public double get90thPercentile() {
    return percentiles[PERCENTILE_90TH];
  }

  public double get95thPercentile() {
    return percentiles[PERCENTILE_95TH];
  }

  public double get99thPercentile() {
    return percentiles[PERCENTILE_99TH];
  }

  public double getMaximum() {
    return percentiles[MAXIMUM];
  }

  public static Statistics computeStatistics(int[] valuesAsMicroseconds) {
    if (valuesAsMicroseconds.length == 0) {
      long[] percentiles = new long[PERCENTILES.length];
      Arrays.fill(percentiles, -1);
      return new Statistics(0, percentiles, -1, -1);
    }

    Arrays.sort(valuesAsMicroseconds);

    double sum = 0;
    for (int value1 : valuesAsMicroseconds) {
      sum += value1;
    }
    double average = sum / valuesAsMicroseconds.length;

    double sumDiffsSquared = 0;
    for (int value : valuesAsMicroseconds) {
      double v = value - average;
      sumDiffsSquared += v * v;
    }
    double standardDeviation = 0;
    if (valuesAsMicroseconds.length > 1) {
      standardDeviation = Math.sqrt(sumDiffsSquared / (valuesAsMicroseconds.length - 1));
    }

    // NOTE: NIST recommends interpolating. This just selects the closest
    // value, which is described as another common technique.
    // http://www.itl.nist.gov/div898/handbook/prc/section2/prc252.htm
    long[] percentiles = new long[PERCENTILES.length];
    for (int i = 0; i < percentiles.length; ++i) {
      int index = (int) (PERCENTILES[i] * valuesAsMicroseconds.length);
      if (index == valuesAsMicroseconds.length) {
        index = valuesAsMicroseconds.length - 1;
      }
      percentiles[i] = valuesAsMicroseconds[index];
    }

    return new Statistics(valuesAsMicroseconds.length, percentiles, average, standardDeviation);
  }
}
