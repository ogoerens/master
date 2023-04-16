package microbench;

import framework.Generator;
import org.apache.commons.math3.analysis.function.Abs;
import org.checkerframework.checker.units.qual.A;
import util.Utils;

import java.util.ArrayList;
import java.util.Random;

public class Queries {
  private static final int scalefactor = 1;
  private static boolean generated = false;
  private static ArrayList<Query> generatedQueries;
  public static String[] tables = {
    "originalCustomer",
    "modifiedCustomer",
    "customer",
    "anonymizedCustomer",
    "Customer_SmallDomain",
    "Customer_Zipf",
    "Customer_Zipf0",
    "Customer_Zipf1",
    "Customer_Zipf2",
    "customerFK",
    "customerPK",
    "newCustomer",
    "newCustomerBiggerDomain",
    "newCustomerRightDomain",
    "customerUnique",
    "customerNonDistinct",
    "customer_nameFixedSize",
    "CustomerExtendedComment",
    "CustomerExtendedComment2",
    "CustomerExtendedCommentFixedSize",
    "CustomerExtendedCommentFixedSize2",
    "customer_ShortenedcommentFixedSize",
    "customer_mktsegZipf2",
    "customer_mktsegZipf3",
    "customer_mktsegZipf",
    "customer_mktsegbinomial",
    "customer_phoneuniform",
    "customer_acctbaluniform",
    "customer_acctbalZipf",
    "customer_acctbalBinomial",
    "customer_numericmktseg",
    "customer_bloatednumericmktseg",
    "customer_bloated2numericmktseg",
    "customer_nationkeyZipf1",
    "customer_nationkeyZipf2",
    "customer_phoneUniformOnlyPrefix",
    "customer_bloatedmktseg",
    "customer_bloated2mktseg",
    "Customer_uncorrelated",
    "Customer_fd",
    "customer_largerNation",
    "customer_smallerNation",
    "customer_custkeyNonDistinct",
    "customer_commentShortened",
    "customer_nationkeyBinomial",
    "customer_nationkeyBinomial2",
    "customer_nationkeyBinomial3",
    "customer_nameshortened",
    "customer_charFD",
    "customer_hexadecimalFD",
    "temp_customer_hexadecimal",
    "customer_hexadecimal",
    "temp_customer_char",
    "customer_char",
    "CustomerClusteredIndex",
    "CustomerNonClusteredIndex",
    "CustomerClusteredIndex_custkeyNonDistinct",
    "customer_commentFixedSize",
    "customer_phone1to1",
    "nation",
    "temporary1",
    "customer_custkeyZipf",
    "customer_custkeyBinomial",
    "Customer_mktsegNonClustered",
    "Customer_mktsegClustered",
    "Customer_mktsegClustered",
    "Customer_mktsegNonClustered",
    "Customer_bloated2MktsegClustered",
    "Customer_bloated2MktsegNonClustered",
    "customer_nationkeyZipfWorstcase"
  };

  public static String[] acctbalDistributionTables = {
    "customer", "customer_acctbaluniform", "customer_acctbalzipf", "customer_acctbalBinomial"
  };
  public static String[] custkeyDistributionTables = {
    "customer", "customer_custkeyZipf", "customer_custkeyBinomial"
  };
  public static String[] mktsegDistributionTables = {
    "customer", "customer_mktsegZipf", "customer_mktsegBinomial", "customer_bloatedmktseg"
  };
  public static String[] nationkeyDistributionTables = {
    "customer",
    "customer_nationkeyZipf1",
    "customer_nationkeyZipf2",
    "customer_nationkeyZipfWorstcase",
    "customer_nationkeyBinomial",
    "customer_nationkeyBinomial2",
    "customer_nationkeyBinomial3"
  };
  public static String[] cardinalityTables = {
    "Customer", "customer_bloatedMktseg", "customer_bloated2Mktseg"
  };
  public static String[] indexTablesCustkey = {
    "Customer", "customerClusteredIndex", "customerNonClusteredIndex"
  };

  public static String[] correlationTablesCorr = {
    "customer", "customer_uncorrelated", "customer_fd"
  };
  public static AbstractQuery aq0 =
      new AbstractQuery("SELECT * FROM %s WHERE c_mktsegment = '%s'", "q0");
  public static String[] aq0Tables = mktsegDistributionTables;
  public static AbstractQuery aq1 =
      new AbstractQuery("SELECT * FROM  %s  WHERE c_nationkey = %s", "q1");
  public static String[] aq1Tables = nationkeyDistributionTables;
  public static AbstractQuery aq2 =
      new AbstractQuery("SELECT * FROM %1$s WHERE c_nationkey=%2$s and c_phone like '%3$s'", "q2");
  public static String[] aq2Tables = {"customer", "customer_phoneuniform", "customer_phone1to1"};
  public static AbstractQuery aq3 =
      new AbstractQuery(
          "SELECT count(*) as ccount, cast(round(c_acctbal,0)/ %2$s as int) as acctbalDimension , sum(c_acctbal) "
              + "FROM %1$s "
              + "GROUP BY cast(round(c_acctbal,0)/ %2$s as int)",
          "q3");
  public static String[] aq3Tables = acctbalDistributionTables;
  public static AbstractQuery aq3b =
      new AbstractQuery(
          "SELECT count(*) as ccount, cast(round(c_acctbal,0)/ %2$s as int) as acctbalDimension , sum(c_acctbal) "
              + "FROM  %1$s WHERE c_acctbal > %3$s and c_acctbal < %4$s "
              + "GROUP BY cast(round(c_acctbal,0)/ %2$s as int)",
          "q3b");
  public static AbstractQuery aq4 =
      new AbstractQuery(
          "SELECT c_mktsegment, avg(c_ACCTBAL) " + "FROM %1$s " + "GROUP BY c_mktsegment", "q4");
  public static String[] aq4Tables = {
    "customer",
    "customer_numericmktseg",
    "customer_bloatedmktseg",
    "customer_bloatednumericmktseg",
    "customer_bloated2mktseg",
    "customer_bloated2numericmktseg"
  };
  public static AbstractQuery aq5 =
      new AbstractQuery(
          "SELECT  count(*) " + "FROM %1$s " + "Group by corr1/%2$s, corr2/%2$s", "q5");
  public static String[] aq5Tables = {"customer", "customer_uncorrelated", "customer_fd"};
  public static AbstractQuery aq6 =
      new AbstractQuery("SELECT * FROM %1$s WHERE corr1 > %2$s and corr2 > %3$s", "q6");
  public static String[] aq6Tables = aq5Tables;
  public static AbstractQuery aq7 =
      new AbstractQuery("SELECT distinct c_nationkey FROM %s Where c_custkey%%2!=0", "q7");
  public static String[] aq7Tables = {
    "customer", "customer_largerNation", "customer_smallerNation"
  };
  public static AbstractQuery aq8 = new AbstractQuery("SELECT DISTINCT (c_custkey) From %s ", "q8");
  public static String[] aq8Tables = {"customer", "customerNonDistinct", "customerUnique"};
  public static AbstractQuery aq9 =
      new AbstractQuery("SELECT c_custkey, c_name FROM %1$s WHERE c_name LIKE '%2$s'", "q9");
  public static String[] aq9Tables = {"customer", "customer_nameShortened"};
  public static AbstractQuery aq10 =
      new AbstractQuery("SELECT * FROM %1$s WHERE c_comment LIKE '%2$s'", "q10");
  public static String[] aq10Tables = {
    "customer",
    "customer_commentShortened",
    "customer_commentFixedSize",
    "customerExtendedComment",
    "customerExtendedComment2",
    "customerExtendedCommentFixedSize",
    "customerExtendedCommentFixedSize2",
    "customer_ShortenedcommentFixedSize"
  };
  public static AbstractQuery aq11 =
      new AbstractQuery("SELECT * FROM %1$s WHERE c_custkey < %2$s", "q11");
  public static String[] aq11Tables = indexTablesCustkey;
  public static AbstractQuery aq12 =
      new AbstractQuery(
          "SELECT DISTINCT(c_custkey) from %1$s where c_custkey >%2$s and c_custkey< %3$s", "q12");
  public static String[] aq12Tables = {
    "CustomerClusteredIndex", "CustomerClusteredIndex_custkeyNonDistinct"
  };

  public static AbstractQuery aq13 =
      new AbstractQuery("SELECT * FROM %s where corr1 not like corr2", "q13");
  public static String[] aq13Tables = {
    "customer_char", "customer_charfd", "Customer_hexadecimal", "Customer_hexadecimalfd"
  };
  public static AbstractQuery aq14 =
      new AbstractQuery("SELECT * FROM %s where corr1 != corr2", "q13b");
  public static String[] aq14Tables = aq13Tables;
  public static AbstractQuery aq15 =
      new AbstractQuery("SELECT * FROM %1$s WHERE corr1%%2!=0 and corr2 > %2$s", "q15");
  public static String[] aq15Tables = {"customer", "customer_uncorrelated", "customer_fd"};
  public static AbstractQuery aq16 =
      new AbstractQuery("SELECT * FROM %1$s WHERE c_mktsegment like '%2$s'", "q16");
  public static String[] aq16Tables = cardinalityTables;
  public static AbstractQuery aq17 = aq0.getCopy("q17");
  public static String[] aq17Tables = {"Customer", "customer_bloated2Mktseg"};
  public static String[] aq17Tables2 = {"customer_numericmktseg", "customer_bloated2numericmktseg"};
  public static AbstractQuery aq18 =
      new AbstractQuery("SELECT distinct c_nationkey from %s", "q18");
  public static String[] aq18Tables = {"Customer", "customerFK"};
  public static AbstractQuery aq19 =
      new AbstractQuery("SELECT distinct c_nationkey from %1$s where c_nationkey<%2$s", "aq19");
  public static String[] aq19Tables = aq18Tables;
  public static AbstractQuery aq20 =
      new AbstractQuery("SELECT * from %s where c_nationkey<%2$s", "q20");
  public static String[] aq20Tables = aq18Tables;
  public static AbstractQuery aq21 = aq8.getCopy("q21");
  public static String[] aq21Tables = {
    "Customer", "customerClusteredIndex", "customerUnique", "customerPK"
  };
  public static AbstractQuery aq22 =
      new AbstractQuery("SELECT * FROM %s where c_custkey%%2!=0", "q22");
  public static String[] aq22Tables = {
    "customer", "customerClusteredIndex", "customerNonClusteredIndex"
  };
  public static AbstractQuery aq23 =
      new AbstractQuery(
          "SELECT * FROM %1$s where c_custkey > %2$s and c_custkey < %3$s and c_nationkey = %4$s",
          "q23");
  public static String[] aq23Tables = {
    "customer", "customerClusteredIndex", "customerNonClusteredIndex"
  };
  public static AbstractQuery aq24 =
      new AbstractQuery("SELECT * FROM %s WHERE c_custkey = %s", "q24");
  public static String[] aq24Tables = {"Customer"};
  public static AbstractQuery aq25 =
      new AbstractQuery("SELECT * FROM %s WHERE c_acctbal < %s", "q25");
  public static String[] aq25Tables = acctbalDistributionTables;

  public static AbstractQuery aq26 =
      new AbstractQuery(
          "SELECT count(*) as ccount, avg(c_acctbal) FROM %1$s GROUP BY cast(c_custkey/%2$s as int)",
          "q26");
  public static String[] aq26Tables = {"Customer"};

  public static AbstractQuery aq27 =
      new AbstractQuery("SELECT * FROM %1$s where corr1 < corr2 + %2$s", "q27");
  public static String[] aq27Tables = correlationTablesCorr;

  public static AbstractQuery aq28 =
      new AbstractQuery("SELECT * FROM %1$s where corr1 < corr2 * %2$s", "q28");
  public static String[] aq28Tables = correlationTablesCorr;
  public static AbstractQuery aq29 =
      new AbstractQuery(
          "SELECT * FROM %1$s where corr1 < corr2 + %2$s and corr1 > corr2-%2$s ", "q29");
  public static String[] aq29Tables = correlationTablesCorr;
  public static AbstractQuery aq30 =
      new AbstractQuery("SELECT * FROM %1$s where c_acctbal >= %2$s and c_acctbal < %3$s", "q30");
  public static String[] aq30Tables = {
    "customer", "customer_acctbaluniform", "customer_acctbalzipf", "customer_acctbalBinomial"
  };

  public static AbstractQuery aq31 = aq11.getCopy("q31");
  public static String[] aq31Tables = custkeyDistributionTables;

  public static AbstractQuery aq32 = aq20.getCopy("q32");
  public static String[] aq32Tables = nationkeyDistributionTables;

  public static AbstractQuery aq33 = aq0.getCopy("q33");
  public static String[] aq33Tables = {
    "customer", "customer_mktsegClustered", "customer_mktsegNonClustered"
  };
  public static String[] aq33Tables2 = {
    "customer_bloated2Mktseg",
    "Customer_bloated2MktsegClustered",
    "Customer_bloated2MktsegNonClustered"
  };

  public static AbstractQuery aq34 = aq24.getCopy("q34");
  public static String[] aq34Tables = indexTablesCustkey;
  public static AbstractQuery aq35 =
      new AbstractQuery(
          "SELECT * FROM %1$s where c_nationkey >= %2$s and c_nationkey < %3$s", "q35");
  public static String[] aq35Tables = {
    "customer",
    "customer_nationkeyZipf1",
    "customer_nationkeyZipf2",
    "customer_nationkeyZipfWorstCase",
    "customer_nationkeyBinomial",
    "customer_nationkeyBinomial2",
    "customer_nationkeyBinomial3",
  };
  public static AbstractQuery aq36 =
      new AbstractQuery("SELECT * FROM %s WHERE c_mktsegment < '%s'", "q36");
  public static String[] aq36Tables = mktsegDistributionTables;

  public static AbstractQuery aq37 = aq4.getCopy("q37");
  public static String[] aq37Tables = mktsegDistributionTables;
  public static AbstractQuery aq38 = aq26.getCopy("q38");
  public static String[] aq38Tables = custkeyDistributionTables;
  public static AbstractQuery aq39 =
      new AbstractQuery("Select * from %1$s where corr1 <%2$s", "q39");
  public static String[] aq39Tables = {
    "newCustomer", "newCustomerBiggerDomain", "newCustomerRightDomain"
  };

  public static AbstractQuery aq40 =
      new AbstractQuery(
          "SELECT * FROM %1$s where corr1 >%2$s and corr1 <%3$s and corr2 >%4$s and corr2 <%5$s",
          "q40");
  public static String[] aq40Tables = correlationTablesCorr;

  public static AbstractQuery aq41 =
          new AbstractQuery(
                  "SELECT count(*) FROM %1$s where corr1 >%2$s and corr1 <%3$s and corr2 >%4$s and corr2 <%5$s",
                  "q41");
  public static String[] aq41Tables = correlationTablesCorr;
  /**
   * Generates the queries that are used in the microbenchmark.
   *
   * @return
   */
  public static Query[] generateMicrobenchmarkQueries(int scalefactor) {
    // q0.
    ArrayList<Query> microbenchmarkQueries = new ArrayList<>();
    String[][] aq0Args = Utils.combineArrays(aq0Tables, MicrobenchUtils.mktsegmentValues);
    microbenchmarkQueries.addAll(aq0.generateQueries(aq0Args));
    // q1.
    Generator g = new Generator(new Random());
    // int[] randomNationkey = g.generateUniform(10, 0, 25);
    int[] randomNationkey = {1, 2, 3, 4, 5, 7, 12, 15, 20, 23};
    String[] randomNationkeyString = Utils.convertIntArrayToStrArray(randomNationkey);
    String[][] aq1Args = Utils.combineArrays(aq1Tables, randomNationkeyString);
    microbenchmarkQueries.addAll(aq1.generateQueries(aq1Args));
    // q2.
    String[][] nationPhoneCombinations = {
      {"24", "34-%"}, {"17", "27-%"}, {"21", "31-%"}, {"2", "12-%"}
    };
    String[][] aq2Args = Utils.combineArrayWithArrayOfArray(aq2Tables, nationPhoneCombinations);
    microbenchmarkQueries.addAll(aq2.generateQueries(aq2Args));
    // q3.
    String[] aq3Vals = {"1", "5", "10", "50", "100", "250", "500", "1000", "10000"};
    String[][] aq3Args = Utils.combineArrays(aq3Tables, aq3Vals);
    microbenchmarkQueries.addAll(aq3.generateQueries(aq3Args));
    String[][] aq3bCond = {{"3000", "5999"}};
    String[][] aq3bArgs =
        Utils.combineArrayWithArrayOfArray(
            aq3Tables, Utils.combineArrayWithArrayOfArray(aq3Vals, aq3bCond));
    microbenchmarkQueries.addAll(aq3b.generateQueries(aq3bArgs));
    // q4.
    microbenchmarkQueries.addAll(aq4.generateQueries(aq4Tables));
    // q5.
    String[] aq5Vals = {"1", "5", "10", "50", "100", "250", "500"};
    String[][] aq5Args = Utils.combineArrays(aq5Tables, aq5Vals);
    microbenchmarkQueries.addAll(aq5.generateQueries(aq5Args));
    // q6.
    String[][] aq6Vals = {{"500", "500"}, {"500", "525"}, {"500", "550"}};
    String[][] aq6Args = Utils.combineArrayWithArrayOfArray(aq6Tables, aq6Vals);
    microbenchmarkQueries.addAll(aq6.generateQueries(aq6Args));
    // q7.
    microbenchmarkQueries.addAll(aq7.generateQueries(aq7Tables));
    // q8.
    microbenchmarkQueries.addAll(aq8.generateQueries(aq8Tables));
    // q9.
    String[] aq9Vals = {"%Cust%omer#000000%", "%5%", "%00"};
    String[][] aq9args = Utils.combineArrays(aq9Tables, aq9Vals);
    microbenchmarkQueries.addAll(aq9.generateQueries(aq9args));
    // q10.
    String[] aq10Vals = {"%regular%", "%ace%"};
    String[][] aq10args = Utils.combineArrays(aq10Tables, aq10Vals);
    microbenchmarkQueries.addAll(aq10.generateQueries(aq10args));
    // q11.
    String[] aq11Vals = {"750"};
    String[][] aq11Args = Utils.combineArrays(aq11Tables, aq11Vals);
    microbenchmarkQueries.addAll(aq11.generateQueries(aq11Args));
    // q12.
    String[][] aq12Vals = {{"7500", "15000"}};
    String[][] aq12Args = Utils.combineArrayWithArrayOfArray(aq12Tables, aq12Vals);
    microbenchmarkQueries.addAll(aq12.generateQueries(aq12Args));
    // q13.
    microbenchmarkQueries.addAll(aq13.generateQueries(aq13Tables));
    // q14.
    microbenchmarkQueries.addAll(aq14.generateQueries(aq14Tables));
    // q15. no use.
    // q16.
    String[] aq16Vals = {"%ure"};
    String[][] aq16Args = Utils.combineArrays(aq16Tables, aq16Vals);
    microbenchmarkQueries.addAll(aq16.generateQueries(aq16Args));
    // q17.
    String[][] aq17Args = {
      {"customer", "Automobile"},
      {"customer_bloatedMktseg", "3utomobile"},
      {"customer_bloated2mktseg", "394omobile"},
      {"customer_numericmktseg", "194734533"},
      {"customer_bloated2numericmktseg", "194735759"}
    };
    microbenchmarkQueries.addAll(aq17.generateQueries(aq17Args));
    // q18.
    microbenchmarkQueries.addAll(aq18.generateQueries(aq18Tables));
    // q19.
    String[] aq19Vals = {"10"};
    String[][] aq19Args = Utils.combineArrays(aq19Tables, aq19Vals);
    microbenchmarkQueries.addAll(aq19.generateQueries(aq19Args));
    // q20.
    String[] aq20Vals = {"10"};
    String[][] aq20Args = Utils.combineArrays(aq20Tables, aq20Vals);
    microbenchmarkQueries.addAll(aq20.generateQueries(aq20Args));
    // q21
    microbenchmarkQueries.addAll(aq21.generateQueries(aq21Tables));
    // q22.
    microbenchmarkQueries.addAll(aq22.generateQueries(aq22Tables));
    // q23.
    String[][] aq23Vals = {{"10000", "15000", "13"}};
    String[][] aq23Args = Utils.combineArrayWithArrayOfArray(aq23Tables, aq23Vals);
    microbenchmarkQueries.addAll(aq23.generateQueries(aq23Args));
    // q25.
    String[] aq25Vals = {"0", "2500", "5000", "7500", "9000"};
    String[][] aq25Args = Utils.combineArrays(aq25Tables, aq25Vals);
    microbenchmarkQueries.addAll(aq25.generateQueries(aq25Args));
    // Q30.
    String[][] aq30Vals = {
      {"-1000", "1000"}, {"1000", "3000"}, {"3000", "5000"}, {"5000", "8000"}, {"8000", "11000"}
    };
    String[][] aq30args = Utils.combineArrayWithArrayOfArray(aq30Tables, aq30Vals);
    microbenchmarkQueries.addAll(aq30.generateQueries(aq30args));
    // Q31.
    int[] val31 = {
      10000 * scalefactor,
      30000 * scalefactor,
      50000 * scalefactor,
      75000 * scalefactor,
      100000 * scalefactor
    };
    String[] aq31Vals = Utils.convertIntArrayToStrArray(val31);
    String[][] aq31Args = Utils.combineArrays(aq31Tables, aq31Vals);
    microbenchmarkQueries.addAll(aq31.generateQueries(aq31Args));
    // Q32.
    String[] aq32Vals = randomNationkeyString;
    String[][] aq32Args = Utils.combineArrays(aq32Tables, aq32Vals);
    microbenchmarkQueries.addAll(aq32.generateQueries(aq32Args));
    // Q33
    String[] aq33Val = {"Furniture"};
    String[][] aq33Args = Utils.combineArrays(aq33Tables, aq33Val);
    microbenchmarkQueries.addAll(aq33.generateQueries(aq33Args));
    String[] aq33Val2 = {"394omobile"};
    String[][] aq33Args2 = Utils.combineArrays(aq33Tables2, aq33Val2);
    microbenchmarkQueries.addAll(aq33.generateQueries(aq33Args2));

    // Q34
    String[] aq34Val = {"24568"};
    String[][] aq34Args = Utils.combineArrays(aq34Tables, aq34Val);
    microbenchmarkQueries.addAll(aq34.generateQueries(aq34Args));
    // Q35
    String[][] aq35Vals = {{"0", "5"}, {"5", "10"}, {"10", "15"}, {"15", "20"}, {"20", "25"}};
    String[][] aq35Args = Utils.combineArrayWithArrayOfArray(aq35Tables, aq35Vals);
    microbenchmarkQueries.addAll(aq35.generateQueries(aq35Args));

    // Q36
    String[] aq36Vals = {"BA", "CA", "GA", "IA", "XY"};
    String[][] aq36Args = Utils.combineArrays(aq36Tables, aq36Vals);
    microbenchmarkQueries.addAll(aq36.generateQueries(aq36Args));
    // Q37
    microbenchmarkQueries.addAll(aq37.generateQueries(aq37Tables));
    // Q38
    String[] aq38vals = {"1"};
    String[][] aq38args = Utils.combineArrays(aq38Tables, aq38vals);
    microbenchmarkQueries.addAll(aq38.generateQueries(aq38args));

    // Q39
    String[] aq39vals = {"0", "250", "500", "750", "1000", "1500"};
    String[][] aq39args = Utils.combineArrays(aq39Tables, aq39vals);
    microbenchmarkQueries.addAll(aq39.generateQueries(aq39args));

    // Q27
    String[] aq27vals = {"0", "1", "25", "50", "100"};
    String[][] aq27args = Utils.combineArrays(aq27Tables, aq27vals);
    microbenchmarkQueries.addAll(aq27.generateQueries(aq27args));

    // Q28
    String[] aq28vals = {"0.5", "0.75", "1", "1.25", "1.5"};
    String[][] aq28args = Utils.combineArrays(aq28Tables, aq28vals);
    microbenchmarkQueries.addAll(aq28.generateQueries(aq28args));

    // Q29
    String[] aq29vals = {"1","15", "25", "35", "50"};
    String[][] aq29args = Utils.combineArrays(aq29Tables, aq29vals);
    microbenchmarkQueries.addAll(aq29.generateQueries(aq29args));

    String[][] aq40Vals ={{"100","200","0","300"},{"200","300","100","400"},{"500","600","400","700"}};
    String[][] aq40args = Utils.combineArrayWithArrayOfArray(aq40Tables,aq40Vals);
    microbenchmarkQueries.addAll(aq40.generateQueries(aq40args));

    //Q41.
    String[][] aq41Vals ={{"100","200","0","300"},{"200","300","100","400"},{"500","600","400","700"}};
    String[][] aq41args = Utils.combineArrayWithArrayOfArray(aq41Tables,aq41Vals);
    microbenchmarkQueries.addAll(aq41.generateQueries(aq41args));

    //q29b
    AbstractQuery aq29b = aq29.getCopy("Q29b");
    String[] aq29bvals = {"1","10","15","21","25"};
    String[][] aq29bargs = Utils.combineArrays(aq29Tables, aq29bvals);
    microbenchmarkQueries.addAll(aq29b.generateQueries(aq29bargs));


    Query[] queries = new Query[microbenchmarkQueries.size()];
    queries = microbenchmarkQueries.toArray(queries);
    return queries;
  }

  public static Query[] generateQueriesOnTable(String tablename) {
    String[] tables = new String[1];
    tables[0] = tablename;
    // q0.
    ArrayList<Query> microbenchmarkQueries = new ArrayList<>();
    String[][] aq0Args = Utils.combineArrays(tables, MicrobenchUtils.mktsegmentValues);
    microbenchmarkQueries.addAll(aq0.generateQueries(aq0Args));
    // q1.
    Generator g = new Generator(new Random());
    int[] randomNationkey = g.generateUniform(10, 0, 25);
    String[] randomNationkeyString = Utils.convertIntArrayToStrArray(randomNationkey);
    String[][] aq1Args = Utils.combineArrays(tables, randomNationkeyString);
    microbenchmarkQueries.addAll(aq1.generateQueries(aq1Args));
    // q2.
    String[][] nationPhoneCombinations = {
      {"24", "34-%"}, {"17", "27-%"}, {"21", "31-%"}, {"2", "12-%"}
    };
    String[][] aq2Args = Utils.combineArrayWithArrayOfArray(tables, nationPhoneCombinations);
    microbenchmarkQueries.addAll(aq2.generateQueries(aq2Args));
    // q3.
    String[] aq3Vals = {"1", "5", "10", "50", "100", "250", "500", "1000", "10000"};
    String[][] aq3Args = Utils.combineArrays(tables, aq3Vals);
    microbenchmarkQueries.addAll(aq3.generateQueries(aq3Args));
    String[][] aq3bCond = {{"3000", "5999"}};
    String[][] aq3bArgs =
        Utils.combineArrayWithArrayOfArray(
            tables, Utils.combineArrayWithArrayOfArray(aq3Vals, aq3bCond));
    microbenchmarkQueries.addAll(aq3b.generateQueries(aq3bArgs));
    // q4.
    microbenchmarkQueries.addAll(aq4.generateQueries(tables));
    // q5.
    String[] aq5Vals = {"1", "5", "10", "50", "100", "250", "500"};
    String[][] aq5Args = Utils.combineArrays(tables, aq5Vals);
    microbenchmarkQueries.addAll(aq5.generateQueries(aq5Args));
    // q6.
    String[][] aq6Vals = {{"500", "500"}, {"500", "525"}, {"500", "550"}};
    String[][] aq6Args = Utils.combineArrayWithArrayOfArray(tables, aq6Vals);
    microbenchmarkQueries.addAll(aq6.generateQueries(aq6Args));
    // q7.
    microbenchmarkQueries.addAll(aq7.generateQueries(tables));
    // q8.
    microbenchmarkQueries.addAll(aq8.generateQueries(tables));
    // q9.
    String[] aq9Vals = {"%Cust%omer#000000%", "%5%", "%00"};
    String[][] aq9args = Utils.combineArrays(tables, aq9Vals);
    microbenchmarkQueries.addAll(aq9.generateQueries(aq9args));
    // q10.
    String[] aq10Vals = {"%regular%", "%ace%"};
    String[][] aq10args = Utils.combineArrays(tables, aq10Vals);
    microbenchmarkQueries.addAll(aq10.generateQueries(aq10args));
    // q11.
    String[] aq11Vals = {"750"};
    String[][] aq11Args = Utils.combineArrays(tables, aq11Vals);
    microbenchmarkQueries.addAll(aq11.generateQueries(aq11Args));
    // q12.
    String[][] aq12Vals = {{"7500", "15000"}};
    String[][] aq12Args = Utils.combineArrayWithArrayOfArray(tables, aq12Vals);
    microbenchmarkQueries.addAll(aq12.generateQueries(aq12Args));
    // q13.
    microbenchmarkQueries.addAll(aq13.generateQueries(tables));
    // q14.
    microbenchmarkQueries.addAll(aq14.generateQueries(tables));
    // q15.
    String[] aq15Vals = {"500"};
    String[][] aq15Args = Utils.combineArrays(tables, aq15Vals);
    microbenchmarkQueries.addAll(aq15.generateQueries(aq15Args));
    // q16.
    String[] aq16Vals = {"%ure"};
    String[][] aq16Args = Utils.combineArrays(tables, aq16Vals);
    microbenchmarkQueries.addAll(aq16.generateQueries(aq16Args));
    // q17.
    String[][] aq17Args = {
      {"customer", "Automobile"},
      {"customer_bloated2mktseg", "394omobile"},
      {"customer_numericmktseg", "194734533"},
      {"customer_bloated2numericmktseg", "194735759"}
    };
    microbenchmarkQueries.addAll(aq17.generateQueries(aq17Args));
    // q18.
    microbenchmarkQueries.addAll(aq18.generateQueries(tables));
    // q19.
    String[] aq19Vals = {"10"};
    String[][] aq19Args = Utils.combineArrays(tables, aq19Vals);
    microbenchmarkQueries.addAll(aq19.generateQueries(aq19Args));
    // q20.
    String[] aq20Vals = {"10"};
    String[][] aq20Args = Utils.combineArrays(tables, aq20Vals);
    microbenchmarkQueries.addAll(aq20.generateQueries(aq20Args));
    // q21
    microbenchmarkQueries.addAll(aq21.generateQueries(tables));
    // q22.
    microbenchmarkQueries.addAll(aq22.generateQueries(tables));
    // q23.
    String[][] aq23Vals = {{"10000", "15000", "13"}};
    String[][] aq23Args = Utils.combineArrayWithArrayOfArray(tables, aq23Vals);
    microbenchmarkQueries.addAll(aq23.generateQueries(aq23Args));

    Query[] queries = new Query[microbenchmarkQueries.size()];
    queries = microbenchmarkQueries.toArray(queries);
    return queries;
  }

  /**
   * Generates a special set of queries that are used to test the different anonymized datasets.
   *
   * @return
   */
  public static ArrayList<Query> generateAnonymizationQueries() {
    // If these queries already have generated once, return the same queries.
    if (microbench.Queries.generated) {
      return Queries.generatedQueries;
    }
    // Generate the queries as they have not been generated yet.
    ArrayList<Query> anonQueries = new ArrayList<>();
    String[] anonymizationTable = {"Customer"};
    Generator g = new Generator(new Random());
    int[] randomNationkey = g.generateUniform(10, 0, 25);
    String[] randomNationkeyString = Utils.convertIntArrayToStrArray(randomNationkey);
    int[] randomCustkey = g.generateUniform(10, 0, 150000 * scalefactor);
    String[] randomCustkeyString = Utils.convertIntArrayToStrArray(randomCustkey);
    int[] randomAcctbal = g.generateUniform(10, -1000, 10000);
    String[] randomAcctbalString = Utils.convertIntArrayToStrArray(randomAcctbal);
    // Equality predicates.
    // Q0.
    String[][] aq0Args = Utils.combineArrays(anonymizationTable, MicrobenchUtils.mktsegmentValues);
    anonQueries.addAll(aq0.generateQueries(aq0Args));
    // Q1.
    String[][] aq1Args = Utils.combineArrays(anonymizationTable, randomNationkeyString);
    anonQueries.addAll(aq1.generateQueries(aq1Args));
    // Q24.
    String[][] aq24Args = Utils.combineArrays(anonymizationTable, randomCustkeyString);
    anonQueries.addAll(aq24.generateQueries(aq24Args));
    // Range predicates.
    // Q11.
    String[][] aq11Args = Utils.combineArrays(anonymizationTable, randomCustkeyString);
    anonQueries.addAll(aq11.generateQueries(aq11Args));
    // Q20.
    String[][] aq20Args = Utils.combineArrays(anonymizationTable, randomNationkeyString);
    anonQueries.addAll(aq20.generateQueries(aq20Args));
    // Q25.
    String[][] aq25Args = Utils.combineArrays(anonymizationTable, randomAcctbalString);
    anonQueries.addAll(aq25.generateQueries(aq25Args));
    // Grouping.
    // Q3.
    String[] aq3Vals = {"1", "5", "10", "50", "100", "250", "500", "1000", "10000"};
    String[][] aq3Args = Utils.combineArrays(anonymizationTable, aq3Vals);
    anonQueries.addAll(aq3.generateQueries(aq3Args));
    // q26.
    String[][] aq26Args = aq3Args;
    anonQueries.addAll(aq26.generateQueries(aq26Args));
    // q27
    String[] aq27Vals = {"1", "5", "10", "20", "30", "50", "100"};
    String[][] aq27Args = Utils.combineArrays(anonymizationTable, aq27Vals);
    anonQueries.addAll(aq27.generateQueries(aq27Args));
    // q28
    String[] aq28Vals = {"1", "1.2", "1.5", "1.75", "2", "5", "10"};
    String[][] aq28Args = Utils.combineArrays(anonymizationTable, aq28Vals);
    anonQueries.addAll(aq28.generateQueries(aq28Args));
    // q29
    String[] aq29Vals = {"1", "5", "10", "20", "30", "50", "100"};
    String[][] aq29Args = Utils.combineArrays(anonymizationTable, aq29Vals);
    anonQueries.addAll(aq29.generateQueries(aq29Args));
    // Q3b.
    // Q4.

    anonQueries.addAll(aq4.generateQueries(anonymizationTable));
    Queries.generatedQueries = anonQueries;
    Queries.generated = true;
    return anonQueries;
  }

  /**
   * Returns the queryset that is given as parameter. Possible querysets are:
   * "original","oldOriginalQueries","microbenchmark", "anonymization". If the name does not match
   * an empty set of queries is returned.
   *
   * @param queryListName
   * @return
   */
  public static Query[] returnQueryList(String queryListName) {
    switch (queryListName) {
      case "oldOriginalQueries":
        return OldQueries.originalQueries;
      case "microbenchmark":
        // return OldQueries.microbenchmarkQueries; Original microbench
        return generateMicrobenchmarkQueries(1);
      case "original":
        return generateQueriesOnTable("customer");
      case "anonymization":
        ArrayList<Query> anonQueries = generateAnonymizationQueries();
        Query[] anonQueriesArray = new Query[anonQueries.size()];
        for (int i = 0; i < anonQueries.size(); i++) {
          anonQueriesArray[i] = anonQueries.get(i);
        }
        return anonQueriesArray;
      default:
        Query[] s = {};
        return s;
    }
  }
}
