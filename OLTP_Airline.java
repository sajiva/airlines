import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class OLTP_Airline extends Thread{
    static String [] trip_type = {"single", "double", "multi"};
    static String [] status_type = {"confirmed", "cancelled"};
    static String [] flight_type = {"domestic", "international"};
    static String [] class_type = {"business", "economy"};

    static int total_flight_instance;
    static int total_business_seats;
    static int total_economy_seats;
    int available_business_seats;
    int available_economy_seats;
    int index_aircraft;
    int customerId;

    DbConnection dbConnection;

    public OLTP_Airline() {
        // Connect to database
        dbConnection = new DbConnection();

    }

    public static void main (String[] args) {

        OLTP_Airline[] threads = new OLTP_Airline[5];

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new OLTP_Airline();
            threads[i].start();
            System.out.println("Thread " + i + " started");
        }

        // Wait for the threads to finish
        for (int i = 0; i < threads.length; i++)
        {
            try
            {
                threads[i].join();
                System.out.println("Thread " + i + " end");
            }
            catch (InterruptedException e)
            {
                System.err.println(e.getMessage());
            }
        }
    }

    public void run() {
        if (!dbConnection.connect()) {
            System.out.println("Cannot connected to database.");
            return;
        }
        customerId = simulate_customer_selection();
        //initial();
        //create_reservation_one(customerId, 1);
        create_reservation(1, "2016-05-06", "single", 1);
        dbConnection.closeConnection();

    }

    public synchronized void create_reservation_one(int customer_id, int seat_num) {
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
        //String column_price = get_column_price(idx_class);
        String column_available_seat = get_column_available_seat(idx_class);

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
                    dbConnection.disableAutoCommit();
                    // insert the reservation
                    sqlQuery = GenerateSQL.insertReservation(customer_id, str_resv_date, str_resv_time, price, str_pay_date, str_pay_time, "single");
                    rs = dbConnection.executeQuery(sqlQuery);
                    if (rs.next()) {
                        index_resv = rs.getInt(1);
                        System.out.println("Reservation Number: " + index_resv + "\n");
                    }

                    // update the flight_leg
                    sqlQuery = GenerateSQL.insertFlightLeg(index_flight_instance, seat_num, class_type[idx_class], index_resv, str_depart_date);
                    rs = dbConnection.executeQuery(sqlQuery);
                    if (rs.next()) {
                        int index_flight_leg = rs.getInt(1);
                        System.out.println("Flight Leg Number: " + index_flight_leg + "\n");
                    }
                    // update the flight instance
                    sqlQuery = GenerateSQL.updateSeatNumMinusOne(column_available_seat, index_flight_instance, str_depart_date);
                    rs = dbConnection.executeQuery(sqlQuery);
                    if (rs.next()) {
                        int available_seat = rs.getInt(1);
                        System.out.println("Available seat: " + available_seat + "\n");
                    }
                    dbConnection.commit();
                    dbConnection.enableAutoCommit();
                } else {
                    System.out.println("Customer " + customer_id + " cannot make reservation because of " + class_type[idx_class] + " seat num = 0");
                }
            } catch (SQLException e) {
                e.printStackTrace();
                System.err.println("Error creating reservation");
                dbConnection.rollback();
                System.exit(0);
            }
        }
    }

    public void create_reservation(int flight_Id, String departDate, String trip_type, int idx_class) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        DateFormat dateFormat1 = new SimpleDateFormat("yyyy-MM-dd");
        // Reservation Date & Time
        Date date_resv = new Date();
        String str_resv_date_time = dateFormat.format(date_resv);
        String[] str_array = str_resv_date_time.split(" ");
        String str_resv_date = str_array[0];
        String str_resv_time = str_array[1];
        // Pay Date & Time
//        Date date_pay = new Date();
//        String str_pay_date_time = dateFormat.format(date_pay);
//        str_array = str_pay_date_time.split(" ");
        String str_pay_date = str_resv_date;
        String str_pay_time = str_resv_time;

        String sqlQuery;
        // reservation table
        double price = 0;
        int available_seat = 0;
        String str_depart_date = str_resv_date;
        int index_resv = 0;

        //int idx_class = simulate_class_selection();
        //String column_price = get_column_price(idx_class);
        String column_available_seat = get_column_available_seat(idx_class);

    //    if(total_flight_instance>0){
            // flight id
    //        int index_flight_instance = simulate_flight_selection();
            try {
                // get the price available seat
                ResultSet rs = get_row_fromDB("flight_instance", "flight_id", flight_Id);

                //ResultSetMetaData metaData = rs.getMetaData();

                if(rs.next()){
                    java.sql.Date depart_date = rs.getDate(2);
                    str_depart_date = dateFormat1.format(depart_date);
                    //available_business_seats = rs.getInt(7);
                    //available_economy_seats = rs.getInt(8);
                    if (idx_class == 0) {
                        price = rs.getInt(9);
                        available_seat = rs.getInt(7);
                    }
                    else {
                        price = rs.getInt(10);
                        available_seat = rs.getInt(8);
                    }
//                    index_aircraft = rs.getInt(12);
//                    System.out.println("Customer " + customer_id + " select Flight " + index_flight_instance + " -> class: " + class_type[idx_class] +
//                            ", Depart Date: " + str_depart_date + ", Available Bus. Seat: " + available_business_seats +
//                            ", Available Eco. Seat: " + available_economy_seats + ", Price: " + price + ", Aircraft: " + index_aircraft + "\n"
//                    );

                    //boolean flag_insert = get_insert_flag(idx_class);
                    if (available_seat > 0) {
                        dbConnection.disableAutoCommit();
                        // insert the reservation
                        index_resv = addReservation(str_resv_date, str_resv_time, price, str_pay_date, str_pay_time, trip_type);

                        List reservedSeats = checkSeatNumbersTaken(flight_Id, str_depart_date);
                        int seat_num = simulate_seat_selection(available_seat, reservedSeats);

                        // update the flight_leg
                        addFlightLeg(flight_Id, seat_num, idx_class, index_resv, str_depart_date);
                        // update the flight instance
                        updateFlightInstance(column_available_seat, flight_Id, str_depart_date);
                        dbConnection.commit();
                        dbConnection.enableAutoCommit();
                    } else {
                        System.out.println("Customer " + customerId + " cannot make reservation because of " + class_type[idx_class] + " seat num = 0");
                    }
                } else {
                    System.err.println("Flight " + flight_Id + "on date " + str_depart_date + " is invalid.");
                }


            } catch (SQLException e) {
                e.printStackTrace();
                System.err.println("Error creating reservation");
                dbConnection.rollback();
                System.exit(0);
            }
    //    }
    }

    private void initial() {
        get_total_fight_instance();
    }

    private void get_total_fight_instance() {
        String sqlQuery = GenerateSQL.getTotalCount("flight_instance");
        ResultSet rs = dbConnection.executeQuery(sqlQuery);
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

    public int simulate_class_selection(){
        Random ran = new Random();
        double seed = ran.nextGaussian();
        if(Math.abs(seed)>=3)
            return 0;
        else
            return 1;
    }

    public int simulate_flight_selection(){
        Random ran = new Random();
        return ran.nextInt(total_flight_instance) + 1;
    }

    public String get_column_price(int idx){
        return idx==0? "price_bus":"price_eco";
    }

    public String get_column_available_seat(int idx) {
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

    public synchronized ResultSet get_row_fromDB(String table_name, String key_name, int key_value) {
        String sqlQuery = GenerateSQL.getRow(table_name, key_name, key_value);
        ResultSet rs = dbConnection.executeQuery(sqlQuery);
        return rs;
    }

    public boolean get_insert_flag(int idx) {
        int available_seat = (idx == 0 ? available_business_seats : available_economy_seats);
        if (available_seat > 0)
            return true;
        else
            return false;
    }

    public int simulate_customer_selection() {
        Random ran = new Random();
        return ran.nextInt(1000) + 1;
    }

    public synchronized int simulate_seat_selection(int available_seats, List reservedSeats) {

        Random ran = new Random();
        int seat_nr = ran.nextInt(available_seats) + 1;
        while (reservedSeats.contains(seat_nr)) {
            seat_nr = ran.nextInt(available_seats) + 1;
        }

        return seat_nr;
    }

    public synchronized List checkSeatNumbersTaken(int flight_id, String depart_date) {
        String sqlQuery = GenerateSQL.getReservedSeatNumbers(flight_id, depart_date);
        ResultSet rs = dbConnection.executeQuery(sqlQuery);
        List reservedSeats = new ArrayList<>();
        //int i = 1;
        try {
            while (rs.next()) {
                reservedSeats.add(rs.getInt(1));
                //i++;
            }
        } catch (SQLException e) {
            System.err.println("Error getting reserved seats");
            e.printStackTrace();
        }
        return reservedSeats;
    }

    public int addReservation(String str_resv_date, String str_resv_time, double price, String str_pay_date, String str_pay_time, String trip_type) throws SQLException {
        String sqlQuery = GenerateSQL.insertReservation(customerId, str_resv_date, str_resv_time, price, str_pay_date, str_pay_time, trip_type);
        ResultSet rs = dbConnection.executeQuery(sqlQuery);
        int index_resv = -1;
        if (rs.next()) {
            index_resv = rs.getInt(1);
            System.out.println("Reservation Number: " + index_resv + "\n");
        }
        return index_resv;
    }

    public void addFlightLeg(int flight_Id, int seat_num, int idx_class, int index_resv, String str_depart_date) throws SQLException {
        String sqlQuery = GenerateSQL.insertFlightLeg(flight_Id, seat_num, class_type[idx_class], index_resv, str_depart_date);
        ResultSet rs = dbConnection.executeQuery(sqlQuery);
        int index_flight_leg = -1;
        if (rs.next()) {
            index_flight_leg = rs.getInt(1);
            System.out.println("Flight Leg Number: " + index_flight_leg + "\n");
        }
    }

    public  synchronized void updateFlightInstance(String column_available_seat, int flight_Id, String str_depart_date) throws SQLException {
        String sqlQuery = GenerateSQL.updateSeatNumMinusOne(column_available_seat, flight_Id, str_depart_date);
        ResultSet rs = dbConnection.executeQuery(sqlQuery);
        if (rs.next()) {
            int available_seats = rs.getInt(1);
            System.out.println("Available seats: " + available_seats + "\n");
        }
    }
}
