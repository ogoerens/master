package util;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Random;
;


public class Utils {
        public static ArrayList<Integer>   gaussianIntegers(int quantity, int mean, int std){
        Random rand = new Random();
        ArrayList<Integer> gaussian_ints = new ArrayList<Integer>(quantity);
        for (int i=0; i<quantity; i++){
            double r = rand.nextGaussian(mean, std);
            int intr= (int) Math.round(r);
            gaussian_ints.add(i,intr);
        }
        return gaussian_ints;
    }

    public static String arrayListToSQLString(ArrayList<String> strs){
        String res="";
        boolean comma=false;
        for (String str : strs) {
            if(comma){
                res+=", "+str;
            }else {
                comma=true;
                res += str;
            }
        }
        return res;
    }

    public static void intArrayToFile(int[] values, String fileName){
        try(PrintStream out = new PrintStream(fileName)){
            for (int x: values){
                out.println(x);
            }
        }catch (FileNotFoundException e){
            System.out.println(e);
        }
    }

    /**
     * Prints a single String to a new file.
     * @param str   The String to be printed.
     * @param fileName  THe file where the String is printed.
     */
    public static void StrToFile(String str, String fileName){
        try(PrintStream out = new PrintStream(fileName)){
            int i=0;
            out.println(str);
        }catch (FileNotFoundException e){
            System.out.println(e);
        }
    }

    /**
     * Stores the contents of a String array in a new file. Each individual String element is printed on a new line.
     * @param values    Array containing the Strings.
     * @param fileName  Name of the file where the array is stored.
     * @param withIndex Indicates if each string element should be preceeded by the index at which the String was placed in the array.
     *                  Index and String are seperated by a blankspace.
     */
    public static void StrArrayToFile(String[] values, String fileName, Boolean withIndex){
        try(PrintStream out = new PrintStream(fileName)){
            int i=0;
            for (String x: values){
                if (withIndex){
                    out.print(i-1+" ");
                    i++;
                }
                out.println(x);
            }
        }catch (FileNotFoundException e){
            System.out.println(e);
        }
    }

    public static void multIntArrayToFile(ArrayList<int[]> values, String fileName, Boolean withIndex){
            int nOfValuesPerArray=values.get(0).length;
        try(PrintStream out = new PrintStream(fileName)){
            for (int i=0;i< nOfValuesPerArray;i++){
                if (withIndex){
                    out.print(i-1+" ");
                }
                for (int j=0; j< values.size();j++){
                    if (j==values.size()-1){
                        out.println(values.get(j)[i]);
                    }else{
                        out.print(values.get(j)[i]+" ");
                    }
                }

            }
        }catch (FileNotFoundException e){
            System.out.println(e);
        }
    }
}
