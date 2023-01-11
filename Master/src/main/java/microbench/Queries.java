package microbench;

public class Queries {

  public static String[] tables = {
    "customer",
    "customerFK",
    "customerPK",
    "newCustomer",
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
    "customer_phoneUniformOnlyPrefix",
    "customer_bloatedmktseg",
    "customer_bloated2mktseg",
    "Customer_uncorrelated",
    "Customer_correlated",
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
    "temporary1"
  };
  public static int count = 3;
  public static String q0 = "SELECT * FROM customer WHERE c_mktsegment = 'Furniture'";
  public static String q0a = "SELECT * FROM customer_mktsegZipf WHERE c_mktsegZipf = 'Furniture'";
  public static String q0b = "SELECT * FROM customer_mktsegZipf2 WHERE c_mktsegZipf = 'Furniture'";
  public static String q0c = "SELECT * FROM customer_mktsegZipf3 WHERE c_mktsegZipf = 'Furniture'";
  public static String q0d =
      "SELECT * FROM customer_mktsegBinomial WHERE c_mktsegBinomial = 'Furniture'";
  public static String q0e =
      "SELECT * FROM customer_bloatedmktseg WHERE c_mktsegBloated like '%iture'";
  public static String q0f = "SELECT * FROM customer WHERE c_mktsegment = 'Automobile'";
  public static String q0g = "SELECT * FROM customer WHERE c_mktsegment = 'Building'";
  public static String q0h = "SELECT * FROM customer WHERE c_mktsegment = 'Machinery'";
  public static String q0i =
      "SELECT * FROM customer_mktsegBinomial WHERE c_mktsegBinomial = 'Automobile'";
  public static String q0j =
      "SELECT * FROM customer_mktsegBinomial WHERE c_mktsegBinomial = 'Building'";
  public static String q0k =
      "SELECT * FROM customer_mktsegBinomial WHERE c_mktsegBinomial = 'Machinery'";
  public static String q0l = "SELECT * FROM customer_mktsegZipf WHERE c_mktsegZipf = 'Building'";
  public static String q1 = "SELECT * FROM  customer  WHERE c_nationkey = 19";
  public static String q1a = "SELECT * FROM  customer_nationkeyZipf1  WHERE c_nationkeyZipf = 1";
  public static String q1b = "SELECT * FROM  customer_nationkeyZipf1  WHERE c_nationkeyZipf = 4";
  public static String q1c = "SELECT * FROM  customer_nationkeyZipf1   WHERE c_nationkeyZipf = 6";
  public static String q1d =
      "SELECT * FROM  customer_nationkeyBinomial   WHERE c_nationkeyBinomial = 8";
  public static String q1e =
      "SELECT * FROM  customer_nationkeyBinomial2   WHERE c_nationkeyBinomial = 17";
  public static String q1f =
      "SELECT * FROM  customer_nationkeyBinomial3   WHERE c_nationkeyBinomial = 8";
  public static String q1g = "SELECT * FROM  customer  WHERE c_nationkey = 9";
  public static String q1h = "SELECT * FROM  customer  WHERE c_nationkey = 20";

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
  public static String q3d =
      "SELECT count(*) as count, cast(round(c_acctbal,0)/ 1000 as int) as acctbalDimension , sum(c_acctbal) "
          + "FROM customer WHERE c_acctbal >3000 and c_acctbal<5999 "
          + "GROUP BY cast(round(c_acctbal,0)/ 1000 as int)";
  public static String q3e =
      "SELECT count(*) as count, cast(round(c_acctbalUniform,0)/ 1000 as int) as acctbalDimension , sum(c_acctbalUniform) "
          + "FROM customer_acctbaluniform WHERE c_acctbalUniform >3000 and c_acctbalUniform<5999 "
          + "GROUP BY cast(round(c_acctbalUniform,0)/ 1000 as int)";
  public static String q3f =
      "SELECT count(*) as count, cast(round(c_acctbalZipf,0)/ 1000 as int) as acctbalDimension , sum(c_acctbalZipf) "
          + "FROM customer_acctbalzipf WHERE c_acctbalZipf >3000 and c_acctbalZipf<5999 "
          + "GROUP BY cast(round(c_acctbalZipf,0)/ 1000 as int)";
  public static String q3g =
      "SELECT count(*) as count, cast(round(c_acctbalBinomial,0)/ 1000 as int) as acctbalDimension , sum(c_acctbalBinomial) "
          + "FROM customer_acctbalBinomial WHERE c_acctbalBinomial >3000 and c_acctbalBinomial<5999 "
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
  public static String q4c =
      "SELECT c_mktsegment, avg(c_ACCTBAL) "
          + "FROM customer_bloatednumericmktseg "
          + "GROUP BY c_mktsegment";

  public static String q4d =
      "SELECT c_mktsegBloated, avg(c_ACCTBAL) "
          + "FROM customer_bloated2mktseg "
          + "GROUP BY c_mktsegBloated";
  public static String q4e =
      "SELECT c_mktsegment, avg(c_ACCTBAL) "
          + "FROM customer_bloated2numericmktseg "
          + "GROUP BY c_mktsegment";

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
  public static String q15 =
      "SELECT * FROM customer_uncorrelated WHERE uniform1/2!=0 and uniform2 > 500";
  public static String q15a = "SELECT * FROM customer_correlated WHERE corr1/2!=0 and corr2 > 500";
  public static String q15b = "SELECT * FROM customer_fd WHERE corr1/2!=0 and corr2 > 1000";
  public static String q7 = "Select distinct c_nationkey FROM customer Where c_custkey%2!=0";
  public static String q7a =
      "Select distinct c_nationkeyBigger FROM customer_largerNation Where c_custkey%2!=0";
  public static String q7b =
      "Select distinct c_nationkeySmaller "
          + "FROM customer_smallerNation "
          + "Where c_custkey%2!=0";
  public static String q8 = "SELECT distinct (c_custkey) From customer ";
  public static String q8a =
      "SELECT distinct (c_custkeyNonDistinct) From customer_custkeyNonDistinct ";
  public static String q9 = "SELECT c_custkey, c_name FROM customer WHERE c_name LIKE '%5%'";

  public static String q9b = "SELECT c_custkey, c_name FROM customer WHERE c_name LIKE '%00'";
  public static String q9a =
      "SELECT c_custkey, c_nameShortened FROM   customer_nameShortened WHERE c_nameShortened LIKE '%5%'";
  public static String q10 = "SELECT * FROM customer WHERE c_comment LIKE '%regular%'";
  public static String q10a = "SELECT * FROM customer WHERE c_comment LIKE '%ace%'";
  public static String q10b =
      "SELECT * FROM customer_commentShortened WHERE c_commentShortened LIKE '%regular%'";
  public static String q10c =
      "SELECT * FROM customer_commentShortened WHERE c_commentShortened LIKE '%ace%'";
  public static String q10d =
      "SELECT * FROM customer_commentFixedSize where c_comment like '%regular%'";
  public static String q10e =
      "SELECT * FROM customer_commentFixedSize where c_comment like '%ace%'";
  // Queries with Index
  public static String q11 = "SELECT * FROM customer where c_custkey< 750";
  public static String q11a = "SELECT * FROM customerClusteredIndex where c_custkey< 750";
  public static String q11b = "SELECT * FROM customerNonClusteredIndex where c_custkey< 750";
  public static String q12 = "SELECT DISTINCT(c_custkey) from customerClusteredIndex";
  public static String q12a =
      "SELECT DISTINCT(c_custkeyNonDistinct)FROM CustomerClusteredIndex_custkeyNonDistinct";
  public static String q13 = "SELECT * FROM Customer_char where corr1 not like corr2";
  public static String q13a = "SELECT * FROM Customer_charFD where corr1 not like corr2";
  public static String q13b = "SELECT * FROM Customer_char where corr1 != corr2";
  public static String q14 = "SELECT * FROM Customer_hexadecimal where corr1 not like corr2";
  public static String q14a = "SELECT * FROM Customer_hexadecimalFD where corr1 not like corr2";
  public static String q16 = "SELECT * FROM customer WHERE c_mktsegment like '%ure'";
  public static String q16a =
      "SELECT * FROM customer_bloatedMktseg WHERE c_mktsegBloated like '%ure'";
  public static String q16b =
      "SELECT * FROM customer_bloated2Mktseg WHERE c_mktsegBloated like '%ure'";
  public static String q17 = "SELECT c_mktsegment FROM customer WHERE c_mktsegment = 'Automobile'";
  public static String q17a =
      "SELECT c_mktsegBloated FROM customer_bloated2mktseg WHERE c_mktsegBloated = '394omobile'";
  public static String q17b =
      "SELECT c_mktsegment FROM customer_numericmktseg WHERE c_mktsegment = 194734533";
  public static String q17c =
      "SELECT c_mktsegment FROM customer_bloated2numericmktseg WHERE c_mktsegment = 194735759";
  public static String q17d =
      "SELECT c_mktsegment FROM customer WHERE c_mktsegment like 'Automobile'";
  public static String q18 = "SELECT distinct c_nationkey from customer";
  public static String q18a = "SELECT distinct c_nationkey from customerFK";
  public static String q19 = "SELECT distinct c_nationkey from customer where c_nationkey<10";
  public static String q19a = "SELECT distinct c_nationkey from customerFK where c_nationkey<10";
  public static String q20 = "SELECT * from customer where c_nationkey<10";
  public static String q20a = "SELECT * from customerFK where c_nationkey<10";
  public static String q21 = "SELECT distinct c_custkey FROM customer";
  public static String q21a = "SELECT distinct c_custkey FROM customerPK";
  public static String q21b = "SELECT distinct c_custkey FROM customerClusteredIndex";
  public static String q22 = "SELECT * FROM customer where c_custkey%2!=0";
  public static String q22a = "SELECT * FROM customerClusteredIndex where c_custkey%2!=0";
  public static String q22b = "SELECT * FROM customerNonClusteredIndex where c_custkey%2!=0";
  public static String q23 =
      "SELECT * FROM customer where c_custkey>10000 and c_custkey<15000 and c_nationkey=13";
  public static String q23a =
      "SELECT * FROM customerClusteredIndex where c_custkey>10000 and c_custkey<15000 and c_nationkey=13";
  public static String q23b =
      "SELECT * FROM customerNonClusteredIndex where c_custkey>10000 and c_custkey<15000 and c_nationkey=13";
  public static String[] queryList = {
    q0, q0a, q0b, q0c, q0d, q0e, q0f, q0g, q0h, q0i, q0j, q0k, q0l, q1, q1a, q1b, q1c, q1d, q1e,
    q1f, q1g, q1h, q2, q2a, q2b, q3, q3a, q3b, q3c, q3d, q3e, q3f, q3g, q4, q4a, q4b, q4c, q4d, q4e,
    q5, q5a, q5b, q6, q6a, q6b, q7, q7a, q7b, q8, q8a, q9, q9a, q9b, q10, q10a, q10b, q10c, q10d,
    q10e, q11, q11a, q11b, q12, q12a, q13, q13a, q13b, q14, q14a, q15, q15a, q15b, q16, q16a, q16b,
    q17, q17a, q17b, q17c, q17d, q18, q18a, q19, q19a, q20, q20a, q21, q21a, q21b, q22, q22a, q22b,
    q23, q23a, q23b
  };
  public static String[] queryListNames = {
    "q0", "q0a", "q0b", "q0c", "q0d", "q0e", "q0f", "q0g", "q0h", "q0i", "q0j", "q0k", "q0l", "q1",
    "q1a", "q1b", "q1c", "q1d", "q1e", "q1f", "q1g", "q1h", "q2", "q2a", "q2b", "q3", "q3a", "q3b",
    "q3c", "q3d", "q3e", "q3f", "q3g", "q4", "q4a", "q4b", "q4c", "q4d", "q4e", "q5", "q5a", "q5b",
    "q6", "q6a", "q6b", "q7", "q7a", "q7b", "q8", "q8a", "q9", "q9a", "q9b", "q10", "q10a", "q10b",
    "q10c", "q10d", "q10e", "q11", "q11a", "q11b", "q12", "q12a", "q13", "q13a", "q13b", "q14",
    "q14a", "q15", "q15a", "q15b", "q16", "q16a", "q16b", "q17", "q17a", "q17b", "q17c", "q17d",
    "q18", "q18a", "q19", "q19a", "q20", "q20a", "q21", "q21a", "q21b", "q22", "q22a", "q22b",
    "q23", "q23a", "q23b"
  };
}
