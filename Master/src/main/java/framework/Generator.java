package framework;

import java.io.File;
import java.io.FileNotFoundException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Scanner;


import microbench.utils;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.math3.linear.BlockRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.random.*;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import util.Utils;



public class Generator {

    private Random rand;
    private RealMatrix covMat;

    public Generator(Random r){
        this.rand=r;
        double[][] values ={{2,2},{2,2}};
        covMat = new BlockRealMatrix(values);
        covMat.createMatrix(2,2);

    }

    public void generate(XMLConfiguration conf){
        int amount = conf.getInt("amount");
        for (int i=1; i<= amount; i++){
            String str = "gen"+i+"/";
            int size = conf.getInt(str+"size");
            String file = conf.getString(str+"fileName");
            String correlation = conf.getString(str+"/correlation");
            String c=conf.getString(str + "distribution");
            int[] res = new int[1];
            double[] resDouble = new double[1];
            String[] resStr = new String[1];
            String returnType = "int";
            switch (c){
                case "uniform":
                    res= generateUniform(size,conf.getInt(str+"upperbound"));
                    break;
                case "uniform c_acctbal":
                    returnType="double";
                    int[] temp0 = res= generateUniform(size,conf.getInt(str+"upperbound"));
                    resDouble = new double[temp0.length];
                    Random random = new Random();
                    for(int j=0; j<temp0.length;j++){
                        resDouble[j]= temp0[j]/100.0 - random.nextInt(1000);
                    }
                    break;
                case "zipf":
                    res= generateZipf(size, conf.getInt(str + "numberOfElements"), conf.getInt(str + "exponent"));
                    break;
                case "binomial":
                    res= generateBinomial(size, conf.getInt(str+"trials"), conf.getDouble(str+"probabiloity"));
                    break;
                case "binomialMapped":
                    returnType="String";
                    int[] temp = generateBinomial(size, conf.getInt(str+"trials"), conf.getDouble(str+"probability"));
                    resStr = Utils.mapIntArrayToStrArray(utils.mktsegmentValues, temp);
                    break;
                case "zipfMapped":
                    returnType = "String";
                    resStr = generateZipfMappedtoString(size, conf.getStringArray(str +"list"), conf.getInt(str + "exponent"));
                    break;
                case "phoneNumber":
                    resStr=microbench.utils.generatePhoneArray(size);
                    returnType ="String";
                    break;
                default: ;
            }
            if (returnType == "int"){
                if (correlation.equals("correlated")){
                    int[] v= generateCorrelated(res);
                    ArrayList<int[]> arrays = new ArrayList<>();
                    arrays.add(res);
                    arrays.add(v);
                    Utils.multIntArrayToFile(arrays,file,conf.getBoolean(str+"withIndex"));
                } else if (correlation.equals("functional dependent")) {
                    int[] v = generateFunctionalDependency(res, conf.getString(str+"expression"));
                    ArrayList<int[]> arrays = new ArrayList<>();
                    arrays.add(res);
                    arrays.add(v);
                    Utils.multIntArrayToFile(arrays,file,conf.getBoolean(str+"withIndex"));
                }else{
                    Utils.intArrayToFile(res, file);
                }
            }else{
                if (returnType == "double"){
                    if (correlation.equals("correlated")){
                        //TODO
                    } else if (correlation.equals("functional dependent")) {
                        //TODO
                    }else{
                        Utils.doubleArrayToFile(resDouble, file, conf.getBoolean(str+"withIndex"));
                    }
                }
                else{
                    //TODO: correlation for string fields
                    Utils.StrArrayToFile(resStr, file, conf.getBoolean(str+"withIndex"));
                }

            }

        }

    }

    public ArrayList<Integer> uniformIntegers(int quantity, int upperbound){
        ArrayList<Integer> random_ints = new ArrayList<Integer>(quantity);
        for (int i=0; i<quantity; i++){
            int r = rand.nextInt(upperbound);
            random_ints.add(i,r);
        }
        return random_ints;
    }

    public int[] generateUniform(int quantity, int upperbound){
        int[] values = new int[quantity];
        UniformIntegerDistribution ud = new UniformIntegerDistribution(0,upperbound);
        return ud.sample(quantity);
    }

    public String[] generateUniformMapped(int quantity, String[] tokens){
        int[] values = new int[quantity];
        int upperbound= tokens.length;
        UniformIntegerDistribution ud = new UniformIntegerDistribution(0,upperbound-1);
        String[] res= new String[quantity];
        for (int i=0; i<quantity; i++){
            res[i]= tokens[ud.sample()];
        }
        return res;
    }


    /**
     * @param v1
     * @param v2
     * @return the PearsonsCorrelation coefficient
     */
    public static double correlationCoeff(double[] v1, double[] v2){
        PearsonsCorrelation pc = new PearsonsCorrelation();
        return pc.correlation(v1,v2);
    }

    /**
     * Genereates an array containing values correlated to the input array
     * @param v original data array
     */
    public int[] generateCorrelated(int[] v){
        int quantity = v.length;
        int[] corr = new int[quantity];
        for (int i=0; i < quantity; i++){
            int x= 100;
            int z = rand.nextInt(x);
            corr[i]= v[i]+(z-(x/2));
        }
        return  corr;
    }

    public int[] generateFunctionalDependency(int[] v, String expr) {
        int quantity = v.length;
        int[] fd = new int[quantity];
        for (int i=0; i < quantity; i++){
            fd[i] = (int) Utils.eval(expr, v[i]);
        }
        return fd;
    }

    public int[] generateZipf(int quantity, int numberOfElements, int exponent){
        ZipfDistribution zd= new ZipfDistribution(numberOfElements, exponent);
        int[] res = new int[quantity];
        for (int i=0; i<quantity; i++){
            res[i]= zd.sample();
        }

        return res;
    }

    /** Creates an array with [quantity] samples of a ZipfianDistribution.
     * @numberOfElements indicates the number of distinct elements in the distribution.
     * @Upperbound indicates the highest possible value for each element
    */
     public int[] generateZipfMappedtoRandom(int quantity, int upperbound, int numberOfElements, int exponent){
        ZipfDistribution zd= new ZipfDistribution(numberOfElements, exponent);
        int[] res = new int[quantity];
        int[] map = new int[numberOfElements];
        for (int i=0;i<numberOfElements; i++){
            map[i]= rand.nextInt(upperbound);
        }
        for (int i=0; i<quantity; i++){
            res[i]= map[zd.sample()-1];
        }
        return res;
    }

    /**
     * @param quantity  indicates the total number  of elements drawn from the distribution
     * @param elements  contains the values of the distribution. Elements at the beginning of the list will have higher cardinality.
     * @param exponent  ,see defintion of ZipfDistribution for definition
     * @return
     */
    public String[] generateZipfMappedtoString(int quantity, String[] elements, int exponent){
        ZipfDistribution zd= new ZipfDistribution(elements.length, exponent);
        String[] res = new String[quantity];
        for (int i=0; i<quantity; i++){
            res[i]= elements[zd.sample()-1];
        }
        return res;
    }

    public int[] generateBinomial(int quantity, int trials, double p){
        BinomialDistribution bd = new BinomialDistribution(trials, p);
        return  bd.sample(quantity);
    }

    public void correlated(){
        double[] mean = {10};
        NormalizedRandomGenerator rr= new NormalizedRandomGenerator() {
            @Override
            public double nextNormalizedDouble() {
                return rand.nextDouble();
            }
        };
        CorrelatedRandomVectorGenerator g = new CorrelatedRandomVectorGenerator(covMat, 1,rr);


        for (int i=0; i<10; i++){
            double[]res = g.nextVector();
            System.out.println(res[0]);
        }
        System.out.println(g.getRootMatrix().toString());

    }

}
