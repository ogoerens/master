package experimental;

import framework.Driver;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.HierarchicalConfiguration;

public class stuff {
    public static void test(){

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
