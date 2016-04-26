import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class GenerateSQL {
//    public static void createLog() throws IOException{
//        FileWriter writer = new FileWriter("log.sql");
//        writer.close();
//    }
//
//    public static void writeLog(String sqlQuery) throws IOException{
//        Files.write(Paths.get("log.sql"), sqlQuery.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
//    }
//
//    public static String getTotalCount(String tableName){
//        String sqlQuery = String.format("SELECT COUNT(*) FROM %s;", tableName);
//        System.out.println(sqlQuery);
//        return  sqlQuery;
//    }
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

//    public static String getRow(String table_name, String key_name, int key_value) {
//        String sqlQuery = String.format("SELECT * FROM %s WHERE %s = %d FOR UPDATE;", table_name, key_name, key_value);
//        System.out.println(sqlQuery);
//        return sqlQuery;
//    }

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

        return sqlQuery;
    }

    public static String insertFlightLeg(int flight_id, int resv_id, String depart_date){
        String sqlQuery = "INSERT INTO flight_leg (flight_id, resv_id, depart_date, flight_type)\n"
                + "VALUES (" + flight_id + "," + resv_id + "," + "\'" + depart_date + "\'" + "," + "\'" + "domestic" + "\'" + ")\n" +
                "RETURNING flight_leg_id;";

        return sqlQuery;
    }

//    public static String updateSeatNumMinusOne(String column_name, int flight_id, String str_depart_date) {
//        String sqlQuery = String.format("UPDATE flight_instance set %s = %s-1\n" +
//                "WHERE flight_id = %d AND depart_date = \'%s\'\n RETURNING %s;", column_name, column_name, flight_id, str_depart_date, column_name);
//        //System.out.println(sqlQuery);
//        return sqlQuery;
//    }

    public static String getAvailableSeatNumbers(int flight_id, String depart_date, int idx_class) {
        String resv_class = (idx_class == 0) ? "business" : "economy";
        String sqlQuery = "SELECT seat_id\n" +
                "FROM seats\n" +
                "WHERE flight_id = " + flight_id + "\n" +
                "AND depart_date = '" + depart_date + "'\n" +
                "AND class = '" + resv_class + "'\n" +
                "AND flight_leg_id is NULL;";
        return sqlQuery;
    }

    public static String getAvailableSeats(int flight_id, String depart_date) {
        String sqlQuery = String.format("SELECT class, COUNT(*)\n" +
                "FROM seats\n" +
                "WHERE flight_id = %d\n" +
                "AND depart_date = '%s'\n" +
                "AND flight_leg_id is NULL\n" +
                "GROUP BY class;", flight_id, depart_date);

        return sqlQuery;
    }

    public static String getTotalSeats(int flight_id, String depart_date) {
        String sqlQuery = String.format("SELECT class, COUNT(*)\n" +
                "FROM seats\n" +
                "WHERE flight_id = %d\n" +
                "AND depart_date = '%s'\n" +
                "GROUP BY class;", flight_id, depart_date);

        return sqlQuery;
    }

    public static String checkSeatNumberAvailable(int flight_id, String depart_date, int seat_nr) {
        String sqlQuery = String.format("SELECT flight_leg_id\n" +
                "FROM seats\n" +
                "WHERE flight_id = %d\n" +
                "AND depart_date = '%s'\n" +
                "AND seat_id = %d FOR UPDATE;", flight_id, depart_date, seat_nr);

        return sqlQuery;
    }

    public static String getPrice(int flight_id, String depart_date) {
        String sqlQuery = String.format("SELECT price_bus,\n" +
                "\tprice_eco\n" +
                "FROM flight_instance\n" +
                "WHERE flight_id = %d\n" +
                "\tAND depart_date = '%s';", flight_id, depart_date);

        return sqlQuery;
    }

    public static String updateSeats(int flight_leg_id, int flight_id, String depart_date, int seat_nr) {
        String sqlQuery = String.format("UPDATE seats\n" +
                "SET flight_leg_id = %d\n" +
                "WHERE flight_id = %d\n" +
                "AND depart_date = '%s'\n" +
                "AND seat_id = %d;", flight_leg_id, flight_id, depart_date, seat_nr);

        return sqlQuery;
    }
}
