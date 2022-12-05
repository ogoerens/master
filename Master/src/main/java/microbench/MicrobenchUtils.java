package microbench;

import util.Utils;

import java.util.Random;

public class MicrobenchUtils {
  public static String[] mktsegmentValues = {
    "FURNITURE", "BUILDING", "HOUSEHOLD", "MACHINERY", "AUTOMOBILE"
  };
  private static int[] phonePrefixes = {
    10, 11, 12, 13, 14, 15, 16, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34
  };
  // c_acctbal range -999.99 to 9999.99


  private static String generatePhoneNumber(int[] prefixes) {
    return generatePhoneNumber(prefixes, false);
  }

  private static String generatePhoneNumber(int[] prefixes, boolean onlyPrefix) {
    String dash = "-";
    Random rand = new Random();
    String pre = Integer.toString(prefixes[rand.nextInt(phonePrefixes.length)]);
    int x = 0;
    String body;
    if (onlyPrefix) {
      body = "000-000-0000";
    } else {
      body =
          Utils.intToFixedSizedString(rand.nextInt(1000), 3)
              + dash
              + Utils.intToFixedSizedString(rand.nextInt(1000), 3)
              + dash
              + Utils.intToFixedSizedString(rand.nextInt(10000), 4);
    }
    return pre + dash + body;
  }

  public static String[] generatePhoneArray(int quantity, boolean onlyPrefix) {
    String[] res = new String[quantity];
    for (int i = 0; i < quantity; i++) {
      res[i] = generatePhoneNumber(phonePrefixes, onlyPrefix);
    }
    return res;
  }
}
