package microbench;

public class queries {
    public static String q0= "SELECT * FROM customer WHERE c_mktsegment = 'Furniture'";
    public static String q0a= "SELECT * FROM resultingCust WHERE corr1 = 'Furniture'";
    public static String qT= "SELECT * FROM customer WHERE c_custkey = 1";
    public static String q1= "SELECT * FROM customer WHERE 'c_nationkey'=24 and 'c_phone'>34";
}
