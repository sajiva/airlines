import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.*;

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
        String sqlQuery = String.format("SELECT COUNT(*) FROM %s;\n", tableName);
        System.out.println(sqlQuery);
        return  sqlQuery;
    }

    public static String getNthRowItem(String tableName, String columnName, int rowNum){
        String sqlQuery = String.format("SELECT %s FROM %s LIMIT 1 OFFSET %s;\n", columnName, tableName, rowNum-1);
        System.out.println(sqlQuery);
        return sqlQuery;
    }

    public static String getRow(String tableName, int rowNum){
        String sqlQuery = String.format("SELECT * FROM %s LIMIT 1 OFFSET %s;\n", tableName, rowNum-1);
        System.out.println(sqlQuery);
        return sqlQuery;
    }

    public static String insertReservation(int customer_id, String str_resv_date, String str_resv_time, double price, String str_pay_date, String str_pay_time, String trip_type){
        String sqlQuery = "INSERT INTO reservation (resv_date, resv_time, resv_status, pay_amount, pay_method, pay_date, pay_time, customer_id, resv_type)\n"
                + "VALUES (" + str_resv_date + " " + str_resv_time + " " + "confirmed" + " " + price + " "+ "card" + " " + str_pay_date + " " + str_pay_time + " " + customer_id + " " + trip_type +");";
        System.out.println(sqlQuery);
        return sqlQuery;
    }

    public static String insertFlightLeg(int flight_id, int seat_num, String class_type, int resv_id, String depart_date){
        String sqlQuery = "INSERT INTO flight_leg (flight_id, seat_num, class, resv_id, depart_date, flight_type)\n"
                + "VALUES (" + flight_id + " " + seat_num + " " + class_type + " " + resv_id + " "+ depart_date + " " + "domestic" +");";
        System.out.println(sqlQuery);
        return sqlQuery;
    }
}
