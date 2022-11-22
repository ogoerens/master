package util;

public class queries {

    public static String bulkInsertStmt(String file, String tbl) {
        return bulkInsertStmt(file,tbl,"' '","'0x0A' ");
    }

    public static String bulkInsertStmt(String file, String tbl, String fieldterminator, String rowterminator) {
        String res = String.format("bulk insert %s from %s with ( "
                + "fieldterminator= %s, "
                + "rowterminator= %s )", tbl, file, fieldterminator, rowterminator);
        return res;
    }
}
