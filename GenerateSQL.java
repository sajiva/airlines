/**********************************************************************************************/
/* COSC6340: Database Systems                                                                 */
/* Project 2: ER Model and OLTP for an Airline Database                                       */
/* Project team: Sajiva Pradhan (1007766), Xiang Xu (1356333)                                 */
/**********************************************************************************************/

public class GenerateSQL {

    public static String insertReservation(int customer_id, String str_resv_date, String str_resv_time, double price, String trip_type) {
        String sqlQuery = "INSERT INTO reservation (resv_date, resv_time, resv_status, pay_amount, pay_method, pay_date, pay_time, customer_id, resv_type)\n"
                + "VALUES (\n" +
                "\'" + str_resv_date + "\',\n" +
                "\'" + str_resv_time + "\',\n" +
                "\'" + "confirmed" + "\',\n" +
                price + ",\n" +
                "\'" + "card" + "\',\n" +
                "\'" + str_resv_date + "\',\n" +
                "\'" + str_resv_time + "\',\n" +
                customer_id + ",\n" +
                "\'" + trip_type + "\')\n" +
                "RETURNING resv_id;";

        return sqlQuery;
    }

    public static String insertFlightLeg(int flight_id, int resv_id, String depart_date){
        String sqlQuery = "INSERT INTO flight_leg (flight_id, resv_id, depart_date, flight_type)\n"
                + "VALUES ( " + flight_id + "," + resv_id + ",\'" + depart_date + "\',\'domestic\')\n" +
                "RETURNING flight_leg_id;";

        return sqlQuery;
    }

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
