package microbench;

import java.util.ArrayList;

public class Queries {

  public static String[] tables = {
    "originalCustomer",
    "customer",
    "customerFK",
    "customerPK",
    "newCustomer",
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
    "temporary1"
  };
  public static int count = 3;
  public static Query q0 =
      new Query("SELECT * FROM customer WHERE c_mktsegment = 'Furniture'", "q0");
  public static Query q0a =
      new Query("SELECT * FROM customer_mktsegZipf WHERE c_mktsegZipf = 'Furniture'", "q0a");
  public static Query q0b =
      new Query("SELECT * FROM customer_mktsegZipf2 WHERE c_mktsegZipf = 'Furniture'", "q0b");
  public static Query q0c =
      new Query("SELECT * FROM customer_mktsegZipf3 WHERE c_mktsegZipf = 'Furniture'", "q0c");
  public static Query q0d =
      new Query(
          "SELECT * FROM customer_mktsegBinomial WHERE c_mktsegBinomial = 'Furniture'", "q0d");
  public static Query q0e =
      new Query("SELECT * FROM customer_bloatedmktseg WHERE c_mktsegBloated like '%iture'", "q0e");
  public static Query q0f =
      new Query("SELECT * FROM customer WHERE c_mktsegment = 'Automobile'", "q0f");
  public static Query q0g =
      new Query("SELECT * FROM customer WHERE c_mktsegment = 'Building'", "q0g");
  public static Query q0h =
      new Query("SELECT * FROM customer WHERE c_mktsegment = 'Household'", "q0h");
  public static Query q0i =
      new Query(
          "SELECT * FROM customer_mktsegBinomial WHERE c_mktsegBinomial = 'Automobile'", "q0i");
  public static Query q0j =
      new Query("SELECT * FROM customer_mktsegBinomial WHERE c_mktsegBinomial = 'Building'", "q0j");
  public static Query q0k =
      new Query(
          "SELECT * FROM customer_mktsegBinomial WHERE c_mktsegBinomial = 'Machinery'", "q0k");
  public static Query q0l =
      new Query("SELECT * FROM customer_mktsegZipf WHERE c_mktsegZipf = 'Building'", "q0l");
  public static Query q1 = new Query("SELECT * FROM  customer  WHERE c_nationkey = 19", "q1");
  public static Query q1a =
      new Query("SELECT * FROM  customer_nationkeyZipf1  WHERE c_nationkeyZipf = 1  ", "q1a");
  public static Query q1b =
      new Query("SELECT * FROM  customer_nationkeyZipf1  WHERE c_nationkeyZipf = 4  ", "q1b");
  public static Query q1c =
      new Query("SELECT * FROM  customer_nationkeyZipf1   WHERE c_nationkeyZipf = 6 ", "q1c");
  /*
    public static String q1a = "SELECT * FROM  customer_nationkeyZipf1  WHERE c_nationkeyZipf = 1  OPTION(QUERYTRACEON 8649)";
  public static String q1b = "SELECT * FROM  customer_nationkeyZipf1  WHERE c_nationkeyZipf = 4  OPTION(QUERYTRACEON 8649)";
  public static String q1c = "SELECT * FROM  customer_nationkeyZipf1   WHERE c_nationkeyZipf = 6  OPTION(QUERYTRACEON 8649)";

   */
  public static Query q1d =
      new Query("SELECT * FROM  customer_nationkeyBinomial   WHERE c_nationkeyBinomial = 8", "q1d");
  public static Query q1e =
      new Query(
          "SELECT * FROM  customer_nationkeyBinomial2   WHERE c_nationkeyBinomial = 17", "q1e");
  public static Query q1f =
      new Query(
          "SELECT * FROM  customer_nationkeyBinomial3   WHERE c_nationkeyBinomial = 9", "q1f");
  public static Query q1g = new Query("SELECT * FROM  customer  WHERE c_nationkey = 9", "q1g");
  public static Query q1h = new Query("SELECT * FROM  customer  WHERE c_nationkey = 20", "q1h");

  public static Query q2 =
      new Query("SELECT * FROM customer WHERE c_nationkey=24 and c_phone like '34-%' ", "q2");
  public static Query q2a =
      new Query(
          "SELECT * FROM customer_phoneuniform WHERE c_nationkey=24 and c_phoneUniform like '34-%'",
          "q2a");
  public static Query q2b =
      new Query(
          "SELECT * FROM customer_phone1to1 WHERE c_nationkey=24 and corr1 like '34-%'", "q2b");

  public static Query q3 =
      new Query(
          "SELECT count(*) as ccount, cast(round(c_acctbal,0)/ 1000 as int) as acctbalDimension , sum(c_acctbal) "
              + "FROM customer "
              + "GROUP BY cast(round(c_acctbal,0)/ 1000 as int)",
          "q3");

  public static Query q3a =
      new Query(
          "SELECT count(*) as count, cast(round(c_acctbalUniform,0)/ 1000 as int) as acctbalDimension , sum(c_acctbalUniform) "
              + "FROM customer_acctbaluniform "
              + "GROUP BY cast(round(c_acctbalUniform,0)/ 1000 as int)",
          "q3a");

  public static Query q3b =
      new Query(
          "SELECT count(*) as count, cast(round(c_acctbalZipf,0)/ 1000 as int) as acctbalDimension , sum(c_acctbalZipf) "
              + "FROM customer_acctbalzipf "
              + "GROUP BY cast(round(c_acctbalZipf,0)/ 1000 as int)",
          "q3b");
  public static Query q3c =
      new Query(
          "SELECT count(*) as count, cast(round(c_acctbalBinomial,0)/ 1000 as int) as acctbalDimension , sum(c_acctbalBinomial) "
              + "FROM customer_acctbalBinomial "
              + "GROUP BY cast(round(c_acctbalBinomial,0)/ 1000 as int)",
          "q3c");
  public static Query q3d =
      new Query(
          "SELECT count(*) as count, cast(round(c_acctbal,0)/ 1000 as int) as acctbalDimension , sum(c_acctbal) "
              + "FROM customer WHERE c_acctbal >3000 and c_acctbal<5999 "
              + "GROUP BY cast(round(c_acctbal,0)/ 1000 as int)",
          "q3d");
  public static Query q3e =
      new Query(
          "SELECT count(*) as count, cast(round(c_acctbalUniform,0)/ 1000 as int) as acctbalDimension , sum(c_acctbalUniform) "
              + "FROM customer_acctbaluniform WHERE c_acctbalUniform >3000 and c_acctbalUniform<5999 "
              + "GROUP BY cast(round(c_acctbalUniform,0)/ 1000 as int)",
          "q3e");
  public static Query q3f =
      new Query(
          "SELECT count(*) as count, cast(round(c_acctbalZipf,0)/ 1000 as int) as acctbalDimension , sum(c_acctbalZipf) "
              + "FROM customer_acctbalzipf WHERE c_acctbalZipf >3000 and c_acctbalZipf<5999 "
              + "GROUP BY cast(round(c_acctbalZipf,0)/ 1000 as int)",
          "q3f");
  public static Query q3g =
      new Query(
          "SELECT count(*) as count, cast(round(c_acctbalBinomial,0)/ 1000 as int) as acctbalDimension , sum(c_acctbalBinomial) "
              + "FROM customer_acctbalBinomial WHERE c_acctbalBinomial >3000 and c_acctbalBinomial<5999 "
              + "GROUP BY cast(round(c_acctbalBinomial,0)/ 1000 as int)",
          "q3g");

  public static Query q4 =
      new Query(
          "SELECT c_mktsegment, avg(c_ACCTBAL) " + "FROM customer " + "GROUP BY c_mktsegment",
          "q4");
  public static Query q4a =
      new Query(
          "SELECT c_mktsegment, avg(c_ACCTBAL) "
              + "FROM customer_numericmktseg "
              + "GROUP BY c_mktsegment",
          "q4a");
  public static Query q4b =
      new Query(
          "SELECT c_mktsegBloated, avg(c_ACCTBAL) "
              + "FROM customer_bloatedmktseg "
              + "GROUP BY c_mktsegBloated",
          "q4b");
  public static Query q4c =
      new Query(
          "SELECT c_mktsegment, avg(c_ACCTBAL) "
              + "FROM customer_bloatednumericmktseg "
              + "GROUP BY c_mktsegment",
          "q4c");

  public static Query q4d =
      new Query(
          "SELECT c_mktsegBloated, avg(c_ACCTBAL) "
              + "FROM customer_bloated2mktseg "
              + "GROUP BY c_mktsegBloated",
          "q4d");
  public static Query q4e =
      new Query(
          "SELECT c_mktsegment, avg(c_ACCTBAL) "
              + "FROM customer_bloated2numericmktseg "
              + "GROUP BY c_mktsegment",
          "q4e");

  public static Query q5 =
      new Query(
          "SELECT  count(*) " + "FROM customer_uncorrelated " + "Group by uniform1/5, uniform2/5",
          "q5");
  public static Query q5a =
      new Query("SELECT  count(*) " + "FROM customer " + "Group by corr1/5, corr2/5", "q5a");
  public static Query q5b =
      new Query("SELECT  count(*) " + "FROM customer_fd " + "Group by corr1/5, corr2/5", "q5b");
  public static Query q6 =
      new Query("SELECT * FROM customer_uncorrelated WHERE uniform1 > 20 and uniform2 > 8", "q6");
  public static Query q6a =
      new Query("SELECT * FROM customer WHERE corr1 > 20 and corr2 > 40", "q6a");
  public static Query q6b =
      new Query("SELECT * FROM customer_fd WHERE corr1 > 20 and corr2 > 40", "q6b");
  public static Query q15 =
      new Query(
          "SELECT * FROM customer_uncorrelated WHERE uniform1/2!=0 and uniform2 > 500", "q15");
  public static Query q15a =
      new Query("SELECT * FROM customer WHERE corr1/2!=0 and corr2 > 500", "q15a");
  public static Query q15b =
      new Query("SELECT * FROM customer_fd WHERE corr1/2!=0 and corr2 > 1000", "q15b");
  public static Query q7 =
      new Query("SELECT distinct c_nationkey FROM customer Where c_custkey%2!=0", "q7");
  public static Query q7a =
      new Query(
          "SELECT distinct c_nationkeyBigger FROM customer_largerNation Where c_custkey%2!=0",
          "q7a");
  public static Query q7b =
      new Query(
          "Select distinct c_nationkeySmaller FROM customer_smallerNation Where c_custkey%2!=0",
          "q7b");
  public static Query q8 = new Query("SELECT DISTINCT (c_custkey) From customer ", "q8");
  public static Query q8a = new Query("SELECT DISTINCT (c_custkey) FROM customerUnique ", "q8a");
  public static Query q8b =
      new Query("SELECT DISTINCT (c_custkey) FROM customerNonDistinct ", "q8b");
  public static Query q9 =
      new Query(
          "SELECT c_custkey, c_name FROM customer WHERE c_name LIKE '%Cust%omer#000000%'",
          "q9");
  public static Query q9a =
      new Query(
          "SELECT c_custkey, c_nameShortened FROM   customer_nameShortened WHERE c_nameShortened LIKE '%5%'",
          "q9a");
  public static Query q9b =
      new Query("SELECT c_custkey, c_name FROM customer WHERE c_name LIKE '%00'", "q9b");
  public static Query q9c =
      new Query(
          "SELECT c_custkey, c_nameShortened FROM   customer_nameShortened WHERE c_nameShortened LIKE '%00'",
          "q9c");
  public static Query q9d =
      new Query(
          "SELECT c_custkey, c_name FROM customer_nameFixedSize WHERE c_name LIKE '%5%'", "q9d");
  public static Query q10 =
      new Query("SELECT * FROM customer WHERE c_comment LIKE '%regular%'", "q10");
  public static Query q10a =
      new Query("SELECT * FROM customer WHERE c_comment LIKE '%ace%'", "q10a");
  public static Query q10b =
      new Query(
          "SELECT * FROM customer_commentShortened WHERE c_commentShortened LIKE '%regular%'",
          "q10b");
  public static Query q10c =
      new Query(
          "SELECT * FROM customer_commentShortened WHERE c_commentShortened LIKE '%ace%'", "q10c");
  public static Query q10d =
      new Query("SELECT * FROM customer_commentFixedSize where c_comment like '%regular%'", "q10d");
  public static Query q10e =
      new Query("SELECT * FROM customer_commentFixedSize where c_comment like '%ace%'", "q10e");
  public static Query q10f =
      new Query("SELECT * FROM customerExtendedComment where c_comment like '%regular%'", "q10f");
  public static Query q10g =
      new Query("SELECT * FROM customerExtendedComment2 where c_comment like '%regular%'", "q10g");
  public static Query q10h =
      new Query(
          "SELECT * FROM customerExtendedCommentFixedSize where c_comment like '%regular%'",
          "q10h");
  public static Query q10i =
      new Query(
          "SELECT * FROM customerExtendedCommentFixedSize2 where c_comment like '%regular%'",
          "q10i");
  public static Query q10j =
      new Query(
          "SELECT * FROM customer_ShortenedcommentFixedSize where c_comment like '%regular%'",
          "q10j");
  public static Query q10k =
      new Query(
          "SELECT * FROM customer_ShortenedcommentFixedSize where c_comment like '%ace%'", "q10k");
  // Queries with Index
  public static Query q11 = new Query("SELECT * FROM customer where c_custkey< 750", "q11");
  public static Query q11a =
      new Query("SELECT * FROM customerClusteredIndex where c_custkey< 750", "q11a");
  public static Query q11b =
      new Query("SELECT * FROM customerNonClusteredIndex where c_custkey< 750", "q11b");
  public static Query q12 =
      new Query(
          "SELECT DISTINCT(c_custkey) from customerClusteredIndex where c_custkey >7500 and c_custkey< 15000",
          "q12");
  public static Query q12a =
      new Query(
          "SELECT DISTINCT(c_custkeyNonDistinct) FROM CustomerClusteredIndex_custkeyNonDistinct where c_custkeyNonDistinct > 7500 and c_custkeyNonDistinct < 15000",
          "q12a");
  public static Query q13 =
      new Query("SELECT * FROM Customer_char where corr1 not like corr2", "q13");
  public static Query q13a =
      new Query("SELECT * FROM Customer_charFD where corr1 not like corr2", "q13a");
  public static Query q13b = new Query("SELECT * FROM Customer_char where corr1 != corr2", "q13b");
  public static Query q14 =
      new Query("SELECT * FROM Customer_hexadecimal where corr1 not like corr2", "q14");
  public static Query q14a =
      new Query("SELECT * FROM Customer_hexadecimalFD where corr1 not like corr2", "q14a");
  public static Query q16 =
      new Query("SELECT * FROM customer WHERE c_mktsegment like '%ure'", "q16");
  public static Query q16a =
      new Query("SELECT * FROM customer_bloatedMktseg WHERE c_mktsegBloated like '%ure'", "q16a");
  public static Query q16b =
      new Query("SELECT * FROM customer_bloated2Mktseg WHERE c_mktsegBloated like '%ure'", "q16b");
  public static Query q17 =
      new Query("SELECT c_mktsegment FROM customer WHERE c_mktsegment = 'Automobile'", "q17");
  public static Query q17a =
      new Query(
          "SELECT c_mktsegBloated FROM customer_bloated2mktseg WHERE c_mktsegBloated = '394omobile'",
          "q17a");
  public static Query q17b =
      new Query(
          "SELECT c_mktsegment FROM customer_numericmktseg WHERE c_mktsegment = 194734533", "q17b");
  public static Query q17c =
      new Query(
          "SELECT c_mktsegment FROM customer_bloated2numericmktseg WHERE c_mktsegment = 194735759",
          "q17c");
  public static Query q17d =
      new Query("SELECT c_mktsegment FROM customer WHERE c_mktsegment like 'Automobile'", "q17d");
  public static Query q17e =
      new Query("SELECT c_mktsegment FROM customer WHERE c_mktsegment like 'Automob%'", "q17e");
  public static Query q17f =
      new Query("SELECT c_mktsegment FROM customer WHERE c_mktsegment like '%omobile'", "q17f");
  public static Query q18 = new Query("SELECT distinct c_nationkey from customer", "q18");
  public static Query q18a = new Query("SELECT distinct c_nationkey from customerFK", "q18a");
  public static Query q19 =
      new Query("SELECT distinct c_nationkey from customer where c_nationkey<10", "q19");
  public static Query q19a =
      new Query("SELECT distinct c_nationkey from customerFK where c_nationkey<10", "q19a");
  public static Query q20 = new Query("SELECT * from customer where c_nationkey<10", "q20");
  public static Query q20a = new Query("SELECT * from customerFK where c_nationkey<10", "q20a");
  public static Query q21 = new Query("SELECT distinct c_custkey FROM customer", "q21");
  public static Query q21a = new Query("SELECT distinct c_custkey FROM customerPK", "q21a");
  public static Query q21b =
      new Query("SELECT distinct c_custkey FROM customerClusteredIndex", "q21b");
  public static Query q21c = new Query("SELECT distinct c_custkey FROM customerUnique", "q21c");
  public static Query q22 = new Query("SELECT * FROM customer where c_custkey%2!=0", "q22");
  public static Query q22a =
      new Query("SELECT * FROM customerClusteredIndex where c_custkey%2!=0", "q22a");
  public static Query q22b =
      new Query("SELECT * FROM customerNonClusteredIndex where c_custkey%2!=0", "q22b");
  public static Query q23 =
      new Query(
          "SELECT * FROM customer where c_custkey>10000 and c_custkey<15000 and c_nationkey=13",
          "q23");
  public static Query q23a =
      new Query(
          "SELECT * FROM customerClusteredIndex where c_custkey>10000 and c_custkey<15000 and c_nationkey=13",
          "q23a");
  public static Query q23b =
      new Query(
          "SELECT * FROM customerNonClusteredIndex where c_custkey>10000 and c_custkey<15000 and c_nationkey=13",
          "q23b");

  public static Query q24 =
      new Query("SELECT * FROM customer_char WHERE corr1 > 'q' and corr2 >'q'", "q24");
  public static Query q24a =
      new Query("SELECT * FROM customer_charFD WHERE corr1 > 'q' and corr2 >'q'", "q24a");
  public static Query[] microbenchmarkQueries = {
    q0, q0a, q0b, q0c, q0d, q0e, q0f, q0g, q0h, q0i, q0j, q0k, q0l, q1, q1a, q1b, q1c, q1d, q1e,
    q1f, q1g, q1h, q2, q2a, q2b, q3, q3a, q3b, q3c, q3d, q3e, q3f, q3g, q4, q4a, q4b, q4c, q4d, q4e,
    q5, q5a, q5b, q6, q6a, q6b, q7, q7a, q7b, q8, q8a, q8b, q9, q9a, q9b, q9c, q9d, q10, q10a, q10b,
    q10c, q10d, q10e, q10f, q10g, q10h, q10i, q10j, q10k, q11, q11a, q11b, q12, q12a, q13, q13a,
    q13b, q14, q14a, q15, q15a, q15b, q16, q16a, q16b, q17, q17a, q17b, q17c, q17d, q17e, q17f, q18,
    q18a, q19, q19a, q20, q20a, q21, q21a, q21b, q21c, q22, q22a, q22b, q23, q23a, q23b
  };
  public static String[] queryListNames = {
    "q0", "q0a", "q0b", "q0c", "q0d", "q0e", "q0f", "q0g", "q0h", "q0i", "q0j", "q0k", "q0l", "q1",
    "q1a", "q1b", "q1c", "q1d", "q1e", "q1f", "q1g", "q1h", "q2", "q2a", "q2b", "q3", "q3a", "q3b",
    "q3c", "q3d", "q3e", "q3f", "q3g", "q4", "q4a", "q4b", "q4c", "q4d", "q4e", "q5", "q5a", "q5b",
    "q6", "q6a", "q6b", "q7", "q7a", "q7b", "q8", "q8a", "q8b", "q9", "q9a", "q9b", "q9c", "q9d",
    "q10", "q10a", "q10b", "q10c", "q10d", "q10e", "q10f", "q10g", "q10h", "q10i", "q10j", "q10k",
    "q11", "q11a", "q11b", "q12", "q12a", "q13", "q13a", "q13b", "q14", "q14a", "q15", "q15a",
    "q15b", "q16", "q16a", "q16b", "q17", "q17a", "q17b", "q17c", "q17d", "q17e", "q17f", "q18",
    "q18a", "q19", "q19a", "q20", "q20a", "q21", "q21a", "q21b", "q21c", "q22", "q22a", "q22b",
    "q23", "q23a", "q23b"
  };

  // not included: q5, q6,q15,
  public static Query[] originalQueries = {
    q0, q0f, q0g, q1, q1g, q1h, q2, q3, q4, q5a,q6a,q7, q8, q9, q10, q10a, q11, q15a, q16, q17, q17d, q17e, q18, q19, q20,q21,q22,q23
  };

  public static Query[] returnQueryList(String queryListName) {
    switch (queryListName) {
      case "originalQueries":
        return originalQueries;
      case "microbenchmark":
        return microbenchmarkQueries;
      default:
        Query[] s = {};
        return s;
    }
  }
}
