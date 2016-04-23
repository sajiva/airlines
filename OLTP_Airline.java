import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class OLTP_Airline {
    static String [] trip_type = {"single", "double", "multi"};
    static String [] status_type = {"confirmed", "cancelled"};
    static String [] flight_type = {"domestic", "international"};
    static String [] class_type = {"business", "economy"};

    static int total_flight_instance;
    //static int total_business_seats;
    //static int total_economy_seats;
    static int available_business_seats;
    static int available_economy_seats;
    static int index_aircraft;

    public static void main (String[] args) {

        // Connect to database
        if (!DbConnection.connect()) {
            System.out.println("Cannot connected to database.");
            return;
        }
        initial();
        create_reservation_one(1, 1);
        DbConnection.closeConnection();
    }

    public static void create_reservation_one(int customer_id, int seat_num) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        DateFormat dateFormat1 = new SimpleDateFormat("yyyy-MM-dd");
        // Reservation Date & Time
        Date date_resv = new Date();
        String str_resv_date_time = dateFormat.format(date_resv);
        String[] str_array = str_resv_date_time.split(" ");
        String str_resv_date = str_array[0];
        String str_resv_time = str_array[1];
        // Pay Date & Time
        Date date_pay = new Date();
        String str_pay_date_time = dateFormat.format(date_pay);
        str_array = str_pay_date_time.split(" ");
        String str_pay_date = str_array[0];
        String str_pay_time = str_array[1];

        String sqlQuery;
        // reservation table
        double price = 100;
        String str_depart_date = str_resv_date;
        int index_resv = 0;

        int idx_class = simulate_class_selection();
        String column_price = get_column_price(idx_class);
        String column_available_seat = get_column_availuable_seat(idx_class);

        if(total_flight_instance>0){
            // flight id
            int index_flight_instance = simulate_flight_selection();
            try {
                // get the price available seat
                ResultSet rs = get_row_fromDB("flight_instance", "flight_id", index_flight_instance);

                ResultSetMetaData metaData = rs.getMetaData();

                if(rs.next()){
                    java.sql.Date depart_date = rs.getDate(2);
                    str_depart_date = dateFormat1.format(depart_date);
                    available_business_seats = rs.getInt(7);
                    available_economy_seats = rs.getInt(8);
                    if (idx_class == 0)
                        price = rs.getInt(9);
                    else
                        price = rs.getInt(10);
                    index_aircraft = rs.getInt(12);
                    System.out.println("Customer " + customer_id + " select Flight " + index_flight_instance + " -> class: " + class_type[idx_class] +
                            ", Depart Date: " + str_depart_date + ", Available Bus. Seat: " + available_business_seats +
                            ", Available Eco. Seat: " + available_economy_seats + ", Price: " + price + ", Aircraft: " + index_aircraft + "\n"
                    );
                }

                boolean flag_insert = get_insert_flag(idx_class);
                if (flag_insert) {
                    DbConnection.disableAutoCommit();
                    // insert the reservation
                    sqlQuery = GenerateSQL.insertReservation(customer_id, str_resv_date, str_resv_time, price, str_pay_date, str_pay_time, "single");
                    rs = DbConnection.executeQuery(sqlQuery);
                    if (rs.next()) {
                        index_resv = rs.getInt(1);
                        System.out.println("Reservation Number: " + index_resv + "\n");
                    }

                    // update the flight_leg
                    sqlQuery = GenerateSQL.insertFlightLeg(index_flight_instance, seat_num, class_type[idx_class], index_resv, str_depart_date);
                    rs = DbConnection.executeQuery(sqlQuery);
                    if (rs.next()) {
                        int index_flight_leg = rs.getInt(1);
                        System.out.println("Flight Leg Number: " + index_flight_leg + "\n");
                    }
                    // update the flight instance
                    sqlQuery = GenerateSQL.updateSeatNumMinusOne(column_available_seat, index_flight_instance, str_depart_date);
                    rs = DbConnection.executeQuery(sqlQuery);
                    if (rs.next()) {
                        int available_seat = rs.getInt(1);
                        System.out.println("Available seat: " + available_seat + "\n");
                    }
                    DbConnection.commit();
                    DbConnection.enableAutoCommit();
                } else {
                    System.out.println("Customer " + customer_id + " cannot make reservation because of " + class_type[idx_class] + " seat num = 0");
                }
            } catch (SQLException e) {
                e.printStackTrace();
                System.err.println("Error creating reservation");
                DbConnection.rollback();
                System.exit(0);
            }
        }
    }

    private static void initial() {
        get_total_fight_instance();
    }

    private static void get_total_fight_instance() {
        String sqlQuery = GenerateSQL.getTotalCount("flight_instance");
        ResultSet rs = DbConnection.executeQuery(sqlQuery);
        try {
            if(rs.next()){
                total_flight_instance = rs.getInt(1);
                System.out.println("The total row in flight_instance is " + total_flight_instance);
            }
        } catch (SQLException e) {
            System.err.println("Error getting total flight instance");
            System.exit(0);
        }
    }

    public static int simulate_class_selection(){
        Random ran = new Random();
        double seed = ran.nextGaussian();
        if(Math.abs(seed)>=3)
            return 0;
        else
            return 1;
    }

    public static int simulate_flight_selection(){
        Random ran = new Random();
        return ran.nextInt(total_flight_instance) + 1;
    }

    //public static int simulate_

    public static String get_column_price(int idx){
        return idx==0? "price_bus":"price_eco";
    }

    public static String get_column_availuable_seat(int idx) {
        return idx == 0 ? "available_bus_seat_num" : "available_eco_seat_num";
    }
    /*
    public static double get_price_fromDB(String column_price, int index_flight_instance) throws SQLException{
        String sqlQuery = GenerateSQL.getNthRowItem("flight_instance", column_price, index_flight_instance);
        ResultSet rs = DbConnection.executeQuery(sqlQuery);
        double price = 0;

        if(rs.next()){
            price = rs.getDouble(1);
            System.out.println("The price of " + column_price + " is " + price);
        }

        return price;
    }*/

    public static ResultSet get_row_fromDB(String table_name, String key_name, int key_value) {
        String sqlQuery = GenerateSQL.getRow(table_name, key_name, key_value);
        ResultSet rs = DbConnection.executeQuery(sqlQuery);
        return rs;
    }

    public static boolean get_insert_flag(int idx) {
        int available_seat = (idx == 0 ? available_business_seats : available_economy_seats);
        if (available_seat > 0)
            return true;
        else
            return false;
    }
}
