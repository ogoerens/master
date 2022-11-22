package util;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Random;
;


public class  utils {
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
