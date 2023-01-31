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
    /*
      File f = new File("file12345.txt");
      Scanner sc = new Scanner(f);
      while (sc.hasNextLine()){
        String x = sc.nextLine();
        System.out.println(x);
      }
      */
      /*
      String testQ = "Select * from information_schema.tables";
      String testQ1 = "Create table testTable (keyValue int)";
      String testQ2 = "Insert into testTable values(789)";
      BulkInsert bi = new BulkInsert("'" +sourcePath+"/file12345.txt'","testTable");
      System.out.println(sourcePath);

      String testQ3 = "Select keyValue from testTable";
      Query q = new Query(testQ,-1);
      PreparedStatement stmt = conn.prepareStatement(q.query_stmt);
      ResultSet rs = stmt.executeQuery();
        while (rs.next()) {
          System.out.println(rs.getString(3));
          // do nothing
          // System.out.println("x");
        }
      stmt = conn.prepareStatement(testQ1);
      stmt.executeUpdate();
      bi.update(conn);
       stmt = conn.prepareStatement(testQ2);
      stmt.executeUpdate();
      while (rs.next()) {
        System.out.println(rs.getString(1));
        // do nothing
        // System.out.println("x");
      }
       stmt = conn.prepareStatement(testQ3);
       rs = stmt.executeQuery();
      while (rs.next()) {
        System.out.println(rs.getString(1));
        // do nothing
        // System.out.println("x");
      }

       */


    String queryString = "Select c_comment from customer";
    Query query = new Query(queryString, false);
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



    return;
  }
}
