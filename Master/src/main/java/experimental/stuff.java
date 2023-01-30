package experimental;

import framework.Driver;
import microbench.Query;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.HierarchicalConfiguration;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

public class stuff {
  public static void test(Connection conn) {

    /*
    Scanner scan = new Scanner(System.in);
    //int quantity = scan.nextInt();
    int quantity=10;
    ArrayList<Integer> random_ints = new ArrayList<Integer>(quantity);
    random_ints= utils.randomIntegers(10);

    try{
        FileWriter fWriter = new FileWriter("file12345.txt");
        for (int i=0; i<quantity;i++){
            fWriter.write((i+1)+" "+random_ints.get(i)+""+"\n");
            //System.out.println(random_ints.get(i));
        }

        fWriter.close();
    }
    catch(IOException e) {
        System.out.print(e.getMessage());
    }

    ArrayList<Integer> gaussian= utils.gaussianIntegers(100, 50, 25);
    System.out.println(gaussian.toString());
    */

    String queryString = "Select c_comment from customer";
    Query query = new Query(queryString);
    ArrayList<String> comments = new ArrayList<>();
    try {
      comments = query.runAndStoreFirstArgument(conn);
    } catch (SQLException e) {
      e.printStackTrace();
    }
    long sum = 0;
    int max = 0;
    for (int i =0; i < comments.size();i++) {
      String comment = comments.get(i);
      int length = comment.getBytes().length;
      if (length<80){
        comments.set(i,comment+"123456abc 789defghij");
      }
      while (comments.get(i).getBytes().length<100){
        comments.set(i,comments.get(i)+"123456abc");
      }

      comment = comments.get(i);
      length = comment.getBytes().length;
      sum += length;
      max = max > length ? max : length;
    }
    double average = sum / (double) comments.size();
    System.out.println(average);
    System.out.println(max);
    String[] coco = new String[comments.size()];
    comments.toArray(coco);



    /*
    String dbConfigFile = "src/main/resources/manageconfig.xml";
    XMLConfiguration conf = Driver.buildXMLConfiguration(dbConfigFile);
    HierarchicalConfiguration prop = conf.configurationAt("SQL");
    //HierarchicalConfiguration sub= conf.configurationAt("gen[1]");
    //int c = sub.getInt("exponent");
    System.out.println(prop.size());

    int n= conf.getInt("amount");
    for (int i=0; i<n; i++){
        HierarchicalConfiguration sub= conf.configurationAt("gen["+(i+1)+"]");
        String dist = sub.getString("distribution");
        System.out.println(dist);
    }
     */
    return;
  }
}
