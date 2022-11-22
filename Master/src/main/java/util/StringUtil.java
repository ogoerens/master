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
        return (join(null,delimiter, Arrays.asList(items)));
    }
}
