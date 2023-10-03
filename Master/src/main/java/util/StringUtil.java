package util;

import java.util.Arrays;

public abstract class StringUtil {
  public static String join(String prefix, String delimiter, Iterable<?> items) {
    if (items == null) {
      return ("");
    }
    if (prefix == null) {
      prefix = "";
    }

    StringBuilder sb = new StringBuilder();
    int i = 0;
    for (Object x : items) {
      if (!prefix.isEmpty()) {
        sb.append(prefix);
      }
      sb.append(x != null ? x.toString() : x).append(delimiter);
      i++;
    }
    if (i == 0) {
      return "";
    }
    sb.delete(sb.length() - delimiter.length(), sb.length());

    return sb.toString();
  }

  public static <T> String join(String delimiter, T... items) {
    return (join(null, delimiter, Arrays.asList(items)));
  }

  public static String createFileName(String directory, String fileName, String fileType) {
    return directory + "/" + fileName + "." + fileType;
  }

  /**
   * Removes a specified StrippingString from front an end of a String. If the String  is not surrounded by the specified StrippingString,
   * then the String is returned unchanged.
   *
   * @param str The String from which the StrippingString is removed.
   * @param strippingString The String that is removed.
   * @return
   */
  public static String stripString(String str,String strippingString) {
    if (str.substring(0, 1).equals(strippingString)
        && str.substring(str.length()-1, str.length()).equals(strippingString)) {
      return str.substring(1, str.length() - 1);
    } else {
      return str;
    }
  }
}
