package microbench;

public class Queries {
    public static int count=3;
    public static String q0= "SELECT * FROM customer WHERE c_mktsegment = 'Furniture'";
    public static String q0a= "SELECT * FROM resultingCust WHERE corr1 = 'Furniture'";
    public static String q1= "SELECT * FROM customer WHERE 'c_nationkey'=24 and 'c_phone'>34";
}
