package util;

import java.util.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

public class Utils {

    /**
     * Creates a String by alternating the elements form array1 and array2. A connector is added
     * between the elements with the same index. A delimiter is inserted between elements with
     * different index.
     *
     * @param array1
     * @param array2
     * @param connector String that is insert in between the ith element of array1 and array2.
     * @param delimiter String that is insert after the ith element of array2 and the ith+1 element of
     *     array1. Not inserted after the last element of array2.
     * @return
     */
    public static String alternate2ArraysToString(
            String[] array1, String[] array2, String connector, String delimiter) {
        if (array1.length != array2.length) {
            throw new RuntimeException("Array lengths do not match.");
        }
        StringBuilder stringBuilder = new StringBuilder();
        int numberOfColumns = array1.length;
        for (int i = 0; i < numberOfColumns; i++) {
            String col = array1[i] + connector + array2[i];
            String s = i == numberOfColumns - 1 ? col : col + delimiter;
            stringBuilder.append(s);
        }
        return stringBuilder.toString();
    }
    public static ArrayList<Integer> gaussianIntegers(int quantity, int mean, int std) {
        Random rand = new Random();
        ArrayList<Integer> gaussian_ints = new ArrayList<Integer>(quantity);
        for (int i = 0; i < quantity; i++) {
            double r = rand.nextGaussian(mean, std);
            int intr = (int) Math.round(r);
            gaussian_ints.add(i, intr);
        }
        return gaussian_ints;
    }

    public static String arrayListToSQLString(ArrayList<String> strs) {
        String res = "";
        boolean comma = false;
        for (String str : strs) {
            if (comma) {
                res += ", " + str;
            } else {
                comma = true;
                res += str;
            }
        }
        return res;
    }

    public static String[] bloatCardinality(String[] inputStrings, int multiplicationFactor) {
        String[] resultStrings = new String[inputStrings.length * multiplicationFactor];
        for (int i = 0; i < multiplicationFactor; i++) {
            for (int j = 0; j < inputStrings.length; j++) {
                String newStr =
                        Integer.toString(i) + inputStrings[j].substring(Integer.toString(i).length());
                resultStrings[i * inputStrings.length + j] = newStr;
            }
        }
        return resultStrings;
    }

    public static void convertIntArrayToStrArray() {

    }

    public static void intArrayToFile(int[] values, String fileName, boolean withIndex) {
        try (PrintStream out = new PrintStream(fileName)) {
            int i = 1;
            for (int x : values) {
                if (withIndex) {
                    out.print(i + " ");
                    i++;
                }
                out.println(x);
            }
        } catch (FileNotFoundException e) {
            System.out.println(e);
        }
    }

    public static void doubleArrayToFile(double[] values, String fileName, Boolean withIndex) {
        try (PrintStream out = new PrintStream(fileName)) {
            for (int i = 0; i < values.length; i++) {
                if (withIndex) out.print((i + 1) + " ");
                out.println(values[i]);
            }
        } catch (FileNotFoundException e) {
            System.out.println(e);
        }
    }

    /**
     * Prints a single String to a new file.
     *
     * @param str      The String to be printed.
     * @param fileName THe file where the String is printed.
     */
    public static void StrToFile(String str, String fileName) {
        try (PrintStream out = new PrintStream(fileName)) {
            int i = 0;
            out.println(str);
        } catch (FileNotFoundException e) {
            System.out.println(e);
        }
    }

    /**
     * Stores the contents of a String array in a new file. Each individual String element is printed
     * on a new line.
     *
     * @param values    Array containing the Strings.
     * @param fileName  Name of the file where the array is stored.
     * @param withIndex Indicates if each string element should be preceeded by the index at which the
     *                  String was placed in the array. Index and String are seperated by a blankspace.
     */
    public static void StrArrayToFile(String[] values, String fileName, Boolean withIndex) {
        try (PrintStream out = new PrintStream(fileName)) {
            int i = 0;
            for (String x : values) {
                if (withIndex) {
                    out.print((i + 1) + " ");
                    i++;
                }
                out.println(x);
            }
        } catch (FileNotFoundException e) {
            System.out.println(e);
        }
    }

    public static void storeNestedArray(String[][] array, String fileName, String separatorInnerArrays , String separatorElements) {
        StringBuffer stringBuffer = new StringBuffer();
        for (int j = 0; j < array.length; j++) {
            String[] next = array[j];
            for (int i = 0; i < next.length; i++) {
                String string = next[i];
                stringBuffer.append(string);
                if (i < next.length - 1) {
                    stringBuffer.append(separatorElements);
                }
            }
            if (j < array.length - 1) {
                stringBuffer.append(separatorInnerArrays);
            }
        }
        Utils.StrToFile(stringBuffer.toString(), fileName);
    }

    public static String surroundWithParentheses(String str) {
        return "(" + str + ")";
    }

    public static String StrArrayToString(String[] strings, String separator, boolean parentheses) {
        StringBuilder joinedString = new StringBuilder();
        for (int i = 0; i < strings.length; i++) {
            if (i != strings.length - 1) {
                joinedString.append(strings[i]).append(separator);
            } else {
                joinedString.append(strings[i]);
            }
        }
        if (parentheses) {
            return surroundWithParentheses(joinedString.toString());
        }
        return joinedString.toString();
    }

    public static void multCharArrayToFile(
            ArrayList<char[]> arrays, String fileName, Boolean withIndex) {
        int nOfValuesPerArray = arrays.get(0).length;
        try (PrintStream out = new PrintStream(fileName)) {
            for (int i = 0; i < nOfValuesPerArray; i++) {
                if (withIndex) {
                    out.print((i + 1) + " ");
                }
                for (int j = 0; j < arrays.size(); j++) {
                    if (j == arrays.size() - 1) {
                        out.println(arrays.get(j)[i]);
                    } else {
                        out.print(arrays.get(j)[i] + " ");
                    }
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println(e);
        }
    }
    public static void multStringArrayToFile(
            ArrayList<String[]> arrays, String fileName, Boolean withIndex) {
        int nOfValuesPerArray = arrays.get(0).length;
        try (PrintStream out = new PrintStream(fileName)) {
            for (int i = 0; i < nOfValuesPerArray; i++) {
                if (withIndex) {
                    out.print((i + 1) + " ");
                }
                for (int j = 0; j < arrays.size(); j++) {
                    if (j == arrays.size() - 1) {
                        out.println(arrays.get(j)[i]);
                    } else {
                        out.print(arrays.get(j)[i] + " ");
                    }
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println(e);
        }
    }

    public static void multIntArrayToFile(
            ArrayList<int[]> arrays, String fileName, Boolean withIndex) {
        int nOfValuesPerArray = arrays.get(0).length;
        try (PrintStream out = new PrintStream(fileName)) {
            for (int i = 0; i < nOfValuesPerArray; i++) {
                if (withIndex) {
                    out.print((i + 1) + " ");
                }
                for (int j = 0; j < arrays.size(); j++) {
                    if (j == arrays.size() - 1) {
                        out.println(arrays.get(j)[i]);
                    } else {
                        out.print(arrays.get(j)[i] + " ");
                    }
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println(e);
        }
    }

    public static void storeIntAsChar(
            int[] inputarray, String fileName, Boolean withIndex) {
        try (PrintStream out = new PrintStream(fileName)) {
            for (int j = 0; j < inputarray.length; j++) {
                if (withIndex) {
                    out.print((j + 1) + " ");
                }
                out.println((char) inputarray[j]);
            }
        } catch (FileNotFoundException e) {
            System.out.println(e);
        }
    }

    public static String[] transformIntArrayToHexArray(int[] inputArray) {
        String[] resultArray = new String[inputArray.length];
        for (int i = 0; i < inputArray.length; i++) {
            resultArray[i]= Integer.toHexString(inputArray[i]);
        }
        return resultArray;
    }

    public static void multDoubleArrayToFile(
            ArrayList<double[]> arrays, String fileName, Boolean withIndex) {
        int nOfValuesPerArray = arrays.get(0).length;
        try (PrintStream out = new PrintStream(fileName)) {
            for (int i = 0; i < nOfValuesPerArray; i++) {
                if (withIndex) {
                    out.print((i + 1) + " ");
                }
                for (int j = 0; j < arrays.size(); j++) {
                    if (j == arrays.size() - 1) {
                        out.println(arrays.get(j)[i]);
                    } else {
                        out.print(arrays.get(j)[i] + " ");
                    }
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println(e);
        }
    }

    /**
     * Converts an int array to a double array by shifting the comma to the left.
     *
     * @param inputArray
     * @param shift      Indicates by how many digits the comma is shifted to the left.
     * @return
     */
    public static double[] intArraytoDoubleArray(int[] inputArray, int shift) {
        double[] resultArray = new double[inputArray.length];
        for (int i = 0; i < inputArray.length; i++) {
            resultArray[i] = inputArray[i] / (double) shift;
        }
        return resultArray;
    }

    /**
     * Converts an integer to a String with a fixed number of digits, i.e. the integer will be
     * precceeded with zeroes if necesseary.
     *
     * @param x    Inetger to be converted.
     * @param size Number of digits in the final String.
     * @return
     */
    public static String intToFixedSizedString(int x, int size) {
        int k = (int) Math.pow(10, size - 1);
        boolean found = false;
        String num = "";
        while (!found && k > 1) {
            if (x / k > 0) {
                found = true;
            } else {
                num += 0;
            }
            k /= 10;
        }
        return num + x;
    }

    public static String[] mapIntArrayToStrArray(HashMap<Integer, String> map, int[] intArr) {
        String[] strArr = new String[intArr.length];
        for (int i = 0; i < intArr.length; i++) {
            strArr[i] = map.get(intArr[i]);
        }
        return strArr;
    }

    public static String[] mapIntArrayToStrArray(String[] strs, int[] intArr) {
        String[] strArr = new String[intArr.length];
        for (int i = 0; i < intArr.length; i++) {
            strArr[i] = strs[intArr[i]];
        }
        return strArr;
    }

    public static ArrayList<Integer> fileToIntArrayList(String fileName) {
        ArrayList<Integer> ints = new ArrayList<>();
        try {
            Scanner sc = new Scanner(new File("fileName"));
            while (sc.hasNextInt()) {
                ints.add(sc.nextInt());
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return ints;
    }

    public static int[] randomizeArrayWithFixedStart(int[] inputArray, int outputsize) {
        return randomizeArrayWithFixedStart(inputArray, -1, outputsize);
    }

    /**
     * Places a specified number at the start of the resulting if it is contained in the input array.
     * Randomizes the order of the other array elements. The reordering is done in-place.
     *
     * @param inputArray
     * @param fixedStart Number which should be placed at index 0 of the resulting array if contained
     *                   in the input array.
     * @param outputsize Outputsize gives the size of the resulting array. If outputsize is smaller
     *                   than the size of the input array, then only a subset of the values of the input array are
     *                   returned. If outputsize is larger than the size of the input array an error is thrown.
     * @return
     */
    public static int[] randomizeArrayWithFixedStart(
            int[] inputArray, int fixedStart, int outputsize) {
        if (outputsize > inputArray.length) {
            throw new RuntimeException("Outputsize is larger than size of input array.");
        }
        //
        boolean fixedStartExist = (fixedStart != -1);
        boolean fixedStartPlaced = false;
        for (int i = 0; i < outputsize; i++) {
            Random rand = new Random();
            int index = i + rand.nextInt(inputArray.length - i);
            if (inputArray[index] == fixedStart && fixedStartExist) {
                inputArray[index] = inputArray[i];
                inputArray[i] = inputArray[0];
                inputArray[0] = fixedStart;
                fixedStartPlaced = true;
            } else {
                int swap = inputArray[i];
                inputArray[i] = inputArray[index];
                inputArray[index] = swap;
            }
        }
        // Only return a part of the input array if outputsize is smaller than the length of inputarray.
        // Check if the fixed start was found during the loop. If not, place it at the start.
        if (outputsize < inputArray.length) {
            if (!fixedStartPlaced && fixedStartExist) {
                inputArray[0] = fixedStart;
            }
            return Arrays.copyOfRange(inputArray, 0, outputsize);
        } else {
            return inputArray;
        }
    }

    public static double eval(final String str, int val) {
        return new Object() {
            int pos = -1, ch;

            void nextChar() {
                ch = (++pos < str.length()) ? str.charAt(pos) : -1;
            }

            boolean eat(int charToEat) {
                while (ch == ' ') nextChar();
                if (ch == charToEat) {
                    nextChar();
                    return true;
                }
                return false;
            }

            double parse() {
                nextChar();
                double x = parseExpression();
                if (pos < str.length()) throw new RuntimeException("Unexpected: " + (char) ch);
                return x;
            }

            // Grammar:
            // expression = term | expression `+` term | expression `-` term
            // term = factor | term `*` factor | term `/` factor
            // factor = `+` factor | `-` factor | `(` expression `)` | number
            //        | functionName `(` expression `)` | functionName factor
            //        | factor `^` factor

            double parseExpression() {
                double x = parseTerm();
                for (; ; ) {
                    if (eat('+')) x += parseTerm(); // addition
                    else if (eat('-')) x -= parseTerm(); // subtraction
                    else return x;
                }
            }

            double parseTerm() {
                double x = parseFactor();
                for (; ; ) {
                    if (eat('*')) x *= parseFactor(); // multiplication
                    else if (eat('/')) x /= parseFactor(); // division
                    else return x;
                }
            }

            double parseFactor() {
                if (eat('+')) return +parseFactor(); // unary plus
                if (eat('-')) return -parseFactor(); // unary minus

                double x;
                int startPos = this.pos;
                if (eat('(')) { // parentheses
                    x = parseExpression();
                    if (!eat(')')) throw new RuntimeException("Missing ')'");
                } else if ((ch >= '0' && ch <= '9') || ch == '.') { // numbers
                    while ((ch >= '0' && ch <= '9') || ch == '.') nextChar();
                    x = Double.parseDouble(str.substring(startPos, this.pos));
                } else if (ch >= 'a' && ch <= 'z') { // functions
                    while (ch >= 'a' && ch <= 'z') nextChar();
                    String func = str.substring(startPos, this.pos);
                    if (eat('(')) {
                        if (!eat(')') && !func.equals("x")) {
                            x = parseExpression();
                            if (!eat(')')) throw new RuntimeException("Missing ')' after argument to " + func);
                        } else {
                            x = parseFactor();
                        }
                        if (func.equals("sqrt")) x = Math.sqrt(x);
                        else if (func.equals("sin")) x = Math.sin(Math.toRadians(x));
                        else if (func.equals("cos")) x = Math.cos(Math.toRadians(x));
                        else if (func.equals("tan")) x = Math.tan(Math.toRadians(x));
                        else throw new RuntimeException("Unknown function: " + func);
                    } else x = val;

                } else {
                    throw new RuntimeException("Unexpected: " + (char) ch);
                }

                if (eat('^')) x = Math.pow(x, parseFactor()); // exponentiation

                return x;
            }
        }.parse();
    }
}
