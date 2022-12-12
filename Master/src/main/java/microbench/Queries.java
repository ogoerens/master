package microbench;

public class Queries {

  public static int count = 3;
  public static String q0 = "SELECT * " + "FROM customer " + "WHERE c_mktsegment = 'Furniture'";
  public static String q0a =
      "SELECT * FROM customer_mktsegZipf " + "WHERE c_mktsegZipf = 'Furniture'";
  public static String q0b =
      "SELECT * FROM customer_mktsegZipf2 " + "WHERE c_mktsegZipf = 'Furniture'";
  public static String q0c =
      "SELECT * FROM customer_mktsegZipf3 " + "WHERE c_mktsegZipf = 'Furniture'";
  public static String q0d =
      "SELECT * FROM customer_mktsegBinomial " + "WHERE c_mktsegBinomial = 'Furniture'";
  public static String q0e =
      "SELECT * " + "FROM customer_bloatedmktseg " + "WHERE c_mktsegBloated like '%iture'";
  public static String q1 = "SELECT * FROM  customer  WHERE c_nationkey = 19";
  public static String q1a = "SELECT * FROM  customer_nationkeyZipf1  WHERE c_nationkeyZipf = 1";
  public static String q1b = "SELECT * FROM  customer_nationkeyZipf1  WHERE c_nationkeyZipf = 4";
  public static String q1c = "SELECT * FROM  customer_nationkeyZipf1   WHERE c_nationkeyZipf = 10";
  public static String q1d =
      "SELECT * FROM  customer_nationkeyBinomial   WHERE c_nationkeyBinomial = 10";
  public static String q1e =
      "SELECT * FROM  customer_nationkeyBinomial2   WHERE c_nationkeyBinomial = 10";
  public static String q1f =
      "SELECT * FROM  customer_nationkeyBinomial3   WHERE c_nationkeyBinomial = 10";

  public static String q2 = "SELECT * FROM customer WHERE c_nationkey=24 and c_phone like '34-%' ";
  public static String q2a =
      "SELECT * FROM customer_phoneuniform WHERE c_nationkey=24 and c_phoneUniform like '34-%'";
  public static String q2b =
      "SELECT * FROM customer_phone1to1 WHERE c_nationkey=24 and corr1 like '34-%'";

  public static String q3 =
      "SELECT count(*) as count, cast(round(c_acctbal,0)/ 1000 as int) as acctbalDimension , sum(c_acctbal) "
          + "FROM customer "
          + "GROUP BY cast(round(c_acctbal,0)/ 1000 as int)";

  public static String q3a =
      "SELECT count(*) as count, cast(round(c_acctbalUniform,0)/ 1000 as int) as acctbalDimension , sum(c_acctbalUniform) "
          + "FROM customer_acctbaluniform "
          + "GROUP BY cast(round(c_acctbalUniform,0)/ 1000 as int)";

  public static String q3b =
      "SELECT count(*) as count, cast(round(c_acctbalZipf,0)/ 1000 as int) as acctbalDimension , sum(c_acctbalZipf) "
          + "FROM customer_acctbalzipf "
          + "GROUP BY cast(round(c_acctbalZipf,0)/ 1000 as int)";
  public static String q3c =
      "SELECT count(*) as count, cast(round(c_acctbalBinomial,0)/ 1000 as int) as acctbalDimension , sum(c_acctbalBinomial) "
          + "FROM customer_acctbalBinomial "
          + "GROUP BY cast(round(c_acctbalBinomial,0)/ 1000 as int)";
  public static String q4 =
      "SELECT c_mktsegment, avg(c_ACCTBAL) " + "FROM customer " + "GROUP BY c_mktsegment";
  public static String q4a =
      "SELECT c_mktsegment, avg(c_ACCTBAL) "
          + "FROM customer_numericmktseg "
          + "GROUP BY c_mktsegment";
  public static String q4b =
      "SELECT c_mktsegBloated, avg(c_ACCTBAL) "
          + "FROM customer_bloatedmktseg "
          + "GROUP BY c_mktsegBloated";
  public static String q5 =
      "SELECT  count(*) " + "FROM customer_uncorrelated " + "Group by uniform1/5, uniform2/5";
  public static String q5a =
      "SELECT  count(*) " + "FROM customer_correlated " + "Group by corr1/5, corr2/5";
  public static String q5b =
      "SELECT  count(*) " + "FROM customer_fd " + "Group by corr1/5, corr2/5";
  public static String q6 =
      "SELECT * FROM customer_uncorrelated WHERE uniform1 > 20 and uniform2 > 8";
  public static String q6a = "SELECT * FROM customer_correlated WHERE corr1 > 20 and corr2 > 40";
  public static String q6b = "SELECT * FROM customer_fd WHERE corr1 > 20 and corr2 > 40";

  public static String q7 =
      "Select distinct c_nationkey " + "FROM customer " + "Where c_custkey/2!=0";
  public static String q7a =
      "Select distinct c_nationkeyBigger " + "FROM customer_largerNation " + "Where c_custkey/2!=0";
  public static String q7b =
      "Select distinct c_nationkeySmaller "
          + "FROM customer_smallerNation "
          + "Where c_custkey/2!=0";
  public static String q8 = "SELECT distinct (c_custkey) " + "From customer ";
  public static String q8a =
      "SELECT distinct (c_custkeyNonDistinct) " + "From customer_custkeyNonDistinct ";
  public static String q9 =
      "SELECT c_custkey, c_name " + "FROM customer " + "WHERE c_name LIKE '%5%'";
  public static String q9a =
      "SELECT c_custkey, c_nameShortened "
          + "FROM   customer_nameShortened "
          + "WHERE c_nameShortened LIKE '%5%'";
  public static String q10 = "SELECT * " + "FROM customer " + "WHERE c_comment LIKE '%regular%'";
  public static String q10a =
      "SELECT * " + "FROM customer_commentShortened " + "WHERE c_commentShortened LIKE '%regular%'";
  public static String q10b =
      "SELECT * FROM customer_commentFixedSize where c_comment like '%regular%'";
  // Queries with Index
  public static String q11 = "SELECT * FROM customer where c_custkey< 750";
  public static String q11a = "SELECT * FROM customerClusteredIndex where c_custkey< 750";
  public static String q11b = "SELECT * FROM customerNonClusteredIndex where c_custkey< 750";
  public static String q12 = "SELECT DISTINCT(c_custkey) from customerClusteredIndex";
  public static String q12a =
      "SELECT DISTINCT(c_custkeyNonDistinct)FROM CustomerClusteredIndex_custkeyNonDistinct";
  public static String q13 = "SELECT * FROM Customer_char where corr1 not like corr2";
  public static String q13a = "SELECT * FROM Customer_charFD where corr1 not like corr2";
  public static String q14 = "SELECT * FROM Customer_hexadecimal where corr1 not like corr2";
  public static String q14a = "SELECT * FROM Customer_hexadecimalFD where corr1 not like corr2";

  public static String[] queryList = {
    q0, q0a, q0b, q0c, q0d, q0e, q1, q1a, q1b, q1c, q1d, q1e, q1f, q2, q2a, q2b, q3, q3a, q3b, q3c,
    q4, q4a, q4b, q5, q5a, q5b, q6, q6a, q6b, q7, q7a, q7b, q8, q8a, q9, q9a, q10, q10a, q10b, q11,
    q11a, q11b, q12, q12a, q13, q13a, q14, q14a
  };
  public static String[] queryListNames = {
    "q0", "q0a", "q0b", "q0c", "q0d", "q0e", "q1", "q1a", "q1b", "q1c", "q1d", "q1e", "q1f", "q2",
    "q2a", "q2b", "q3", "q3a", "q3b", "q3c", "q4", "q4a", "q4b", "q5", "q5a", "q5b", "q6", "q6a",
    "q6b", "q7", "q7a", "q7b", "q8", "q8a", "q9", "q9a", "q10", "q10a", "q10b", "q11", "q11a",
    "q11b", "q12", "q12a", "q13", "q13a", "q14", "q14a"
  };
}
