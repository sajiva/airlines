import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Created by xiang on 4/22/16.
 */
public class GenerateSQL {
    public static void createLog() throws IOException{
        FileWriter writer = new FileWriter("log.sql");
        writer.close();
    }

    public static void writeLog(String sqlQuery) throws IOException{
        Files.write(Paths.get("log.sql"), sqlQuery.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    public static String getTotalCount(String tableName){
        String sqlQuery = String.format("SELECT COUNT(*) FROM %s;", tableName);
        System.out.println(sqlQuery);
        return  sqlQuery;
    }
    /*
    public static String getNthRowItem(String tableName, String columnName, int rowNum){
        String sqlQuery = String.format("SELECT %s FROM %s LIMIT 1 OFFSET %s;\n", columnName, tableName, rowNum-1);
        System.out.println(sqlQuery);
        return sqlQuery;
    }*/
    /*
    public static String getRow(String tableName, int rowNum){
        String sqlQuery = String.format("SELECT * FROM %s LIMIT 1 OFFSET %s;\n", tableName, rowNum-1);
        System.out.println(sqlQuery);
        return sqlQuery;
    }*/

    public static String getRow(String table_name, String key_name, int key_value) {
        String sqlQuery = String.format("SELECT * FROM %s WHERE %s = %d FOR UPDATE;", table_name, key_name, key_value);
        System.out.println(sqlQuery);
        return sqlQuery;
    }

    public static String insertReservation(int customer_id, String str_resv_date, String str_resv_time, double price,
                                           String str_pay_date, String str_pay_time, String trip_type) {
        String sqlQuery = "INSERT INTO reservation (resv_date, resv_time, resv_status, pay_amount, pay_method, pay_date, pay_time, customer_id, resv_type)\n"
                + "VALUES (" +
                "\'" + str_resv_date + "\'" + "," +
                "\'" + str_resv_time + "\'" + "," +
                "\'" + "confirmed" + "\'" + "," +
                price + ", " +
                "\'" + "card" + "\'" + "," +
                "\'" + str_pay_date + "\'" + "," +
                "\'" + str_pay_time + "\'" + "," +
                customer_id + "," + "\'" +
                trip_type + "\'" + ")\n" +
                "RETURNING resv_id;";
        //System.out.println(sqlQuery);
        return sqlQuery;
    }

    public static String insertFlightLeg(int flight_id, int seat_num, String class_type, int resv_id, String depart_date){
        String sqlQuery = "INSERT INTO flight_leg (flight_id, seat_num, class, resv_id, depart_date, flight_type)\n"
                + "VALUES (" + flight_id + "," + seat_num + "," + "\'" + class_type + "\'" + "," + resv_id + "," + "\'" + depart_date + "\'" + "," + "\'" + "domestic" + "\'" + ")\n" +
                "RETURNING flight_leg_id;";
        //System.out.println(sqlQuery);
        return sqlQuery;
    }

    public static String updateSeatNumMinusOne(String column_name, int flight_id, String str_depart_date) {
        String sqlQuery = String.format("UPDATE flight_instance set %s = %s-1\n" +
                "WHERE flight_id = %d AND depart_date = \'%s\'\n RETURNING %s;", column_name, column_name, flight_id, str_depart_date, column_name);
        //System.out.println(sqlQuery);
        return sqlQuery;
    }

    public static String getReservedSeatNumbers(int flight_id, String depart_date) {
        String sqlQuery = "SELECT seat_num\n" +
                "FROM flight_leg\n" +
                "WHERE flight_id = " + flight_id + "\n" +
                "AND depart_date = '" + depart_date + "';";

        return sqlQuery;
    }
}
