package util;

import framework.DataManager;

import java.sql.SQLException;
import java.util.ArrayList;

public class DBUtils {
    public static void createTable(DataManager dataManger, ArrayList<String[]> columnNamesAndTypes, String tablename) throws SQLException {
        ArrayList<String> colNamesUppercase = new ArrayList<>();
        ArrayList<String> colTypes = new ArrayList<>();
        for (String[] colNameAndType : columnNamesAndTypes) {
            colNamesUppercase.add(colNameAndType[0].toUpperCase());
            String type = (colNameAndType[1]);
            if (type.contains("char")) {
                colTypes.add(type + " " + Utils.surroundWithParentheses(colNameAndType[2]));
            } else {
                colTypes.add(type);
            }
        }
        String[] cN = new String[colNamesUppercase.size()];
        String[] cT = new String[colTypes.size()];
        dataManger.createTable(tablename, colTypes.toArray(cT), colNamesUppercase.toArray(cN));
    }
}
