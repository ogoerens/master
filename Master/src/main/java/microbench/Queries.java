package microbench;

import java.util.StringTokenizer;

public class Queries {
    public static int count=3;
    public static String q0=
            "SELECT * " +
                    "FROM customer " +
                    "WHERE c_mktsegment = 'Furniture'";
    public static String q1a=
            "SELECT * " +
                    "FROM customer_mktsegZipf " +
                    "WHERE corr1 = 'Furniture'";
    public static String q1b=
            "SELECT * " +
                    "FROM customer_mktsegZipf2 " +
                    "WHERE corr1 = 'Furniture'";
    public static String q1c=
            "SELECT * " +
                    "FROM customer_mktsegZipf3 " +
                    "WHERE corr1 = 'Furniture'";
    public static String q2=
            "SELECT * " +
                    "FROM customer_mktsegBinomial " +
                    "WHERE corr1 = 'Furniture'";
    public static String q3=
            "SELECT * FROM customer WHERE c_nationkey=24 and c_phone like '34-%' ";
    public static String q4=
            "SELECT * " +
                    "FROM customer_phoneuniform " +
                    "WHERE c_nationkey=24 and corr1 like '34-%'";
    public static String q5=
            "SELECT count(*) as count, cast(round(c_acctbal,0)/ 1000 as int) as acctbalDimension , sum(c_acctbal) " +
            "FROM customer " +
            "GROUP BY cast(round(c_acctbal,0)/ 1000 as int)";

    public static String q51=
            "SELECT count(*) as count, cast(round(corr1,0)/ 1000 as int) as acctbalDimension , sum(corr1) " +
                    "FROM customer_acctbaluniform " +
                    "GROUP BY cast(round(corr1,0)/ 1000 as int)";
    public static String q6 =
            "SELECT c_mktsegment, avg(c_ACCTBAL) " +
            "FROM customer " +
            "GROUP BY c_mktsegment";
    public static String q7 =
            "SELECT  count(* ) " +
            "FROM newcustomer " +
            "Group by corr1/5, corr2/5";
    public static String q8 =
            "Select distinct c_nationkey " +
            "FROM customer " +
            "Where c_custkey/2!=0";
    public static String q9=
            "SELECT distinct (c_custkey) " +
            "From customer ";
    public static String q10=
            "SELECT c_custkey, c_name " +
                    "FROM customer " +
                    "WHERE c_name LIKE '%96'";
    public static String q11=
            "SELECT * " +
                    "FROM customer " +
                    "WHERE c_comment LIKE '%regular%'";
    public static String q12=
            "SELECT * " +
                    "FROM customer " +
                    "WHERE corr1 > 20 and corr2 > 8";
}
