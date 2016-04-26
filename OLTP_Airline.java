import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class OLTP_Airline extends Thread{
    static String [] trip_type = {"single", "double", "multi"};
    static String [] status_type = {"confirmed", "cancelled"};
    static String [] flight_type = {"domestic", "international"};
    static String [] class_type = {"business", "economy"};

    private int customerId;
    private DbConnection dbConnection;

    public OLTP_Airline() {
        // Create new database connection
        dbConnection = new DbConnection();
    }

    public static void main (String[] args) {
        /*OLTP_Airline airline = new OLTP_Airline();
        try {
            airline.addRowstoSeats();
        }catch (SQLException e){

        }*/

        int nthreads = 1;
        // create threads
        OLTP_Airline[] threads = new OLTP_Airline[nthreads];

        // start the threads
        for (int i = 0; i < nthreads; i++) {
            // sleep for 10 seconds
            if ((i > 0) && ((i % 40) == 0)) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

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
            return;
        }

        customerId = simulate_customer_selection();
        System.out.println("Customer Id: " + customerId);
        //create_reservation(5, "2016-05-15", "single", 1);
        //initial();
        //create_reservation_one(customerId, 10);

        //System.out.println("Customer Id: " + customerId);
        create_reservation(4, "2016-05-15", "single");
        dbConnection.closeConnection();

    }

//    public void create_reservation_one(int customer_id, int seat_num) {
//        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        DateFormat dateFormat1 = new SimpleDateFormat("yyyy-MM-dd");
//        // Reservation Date & Time
//        Date date_resv = new Date();
//        String str_resv_date_time = dateFormat.format(date_resv);
//        String[] str_array = str_resv_date_time.split(" ");
//        String str_resv_date = str_array[0];
//        String str_resv_time = str_array[1];
//        // Pay Date & Time
//        Date date_pay = new Date();
//        String str_pay_date_time = dateFormat.format(date_pay);
//        str_array = str_pay_date_time.split(" ");
//        String str_pay_date = str_array[0];
//        String str_pay_time = str_array[1];
//
//        String sqlQuery;
//        // reservation table
//        double price = 100;
//        String str_depart_date = str_resv_date;
//        int index_resv = 0;
//
//        int idx_class = simulate_class_selection();
//        //String column_price = get_column_price(idx_class);
//        String column_available_seat = get_column_available_seat(idx_class);
//
//        if(total_flight_instance>0){
//            // flight id
//            int index_flight_instance = simulate_flight_selection();
//            try {
//                // get the price available seat
//                ResultSet rs = get_row_fromDB("flight_instance", "flight_id", index_flight_instance);
//
//                ResultSetMetaData metaData = rs.getMetaData();
//
//                if(rs.next()){
//                    java.sql.Date depart_date = rs.getDate(2);
//                    str_depart_date = dateFormat1.format(depart_date);
//                    available_business_seats = rs.getInt(7);
//                    available_economy_seats = rs.getInt(8);
//                    if (idx_class == 0)
//                        price = rs.getInt(9);
//                    else
//                        price = rs.getInt(10);
//                    int index_aircraft = rs.getInt(12);
//                    System.out.println("Customer " + customer_id + " select Flight " + index_flight_instance + " -> class: " + class_type[idx_class] +
//                            ", Depart Date: " + str_depart_date + ", Available Bus. Seat: " + available_business_seats +
//                            ", Available Eco. Seat: " + available_economy_seats + ", Price: " + price + ", Aircraft: " + index_aircraft + "\n"
//                    );
//                }
//
//                boolean flag_insert = get_insert_flag(idx_class);
//                if (flag_insert) {
//                    dbConnection.disableAutoCommit();
//                    // insert the reservation
//                    sqlQuery = GenerateSQL.insertReservation(customer_id, str_resv_date, str_resv_time, price, str_pay_date, str_pay_time, "single");
//                    rs = dbConnection.executeQuery(sqlQuery);
//                    if (rs.next()) {
//                        index_resv = rs.getInt(1);
//                        System.out.println("Reservation Number: " + index_resv + "\n");
//                    }
//
//                    // update the flight_leg
//                    sqlQuery = GenerateSQL.insertFlightLeg(index_flight_instance, seat_num, class_type[idx_class], index_resv, str_depart_date);
//                    rs = dbConnection.executeQuery(sqlQuery);
//                    if (rs.next()) {
//                        int index_flight_leg = rs.getInt(1);
//                        System.out.println("Flight Leg Number: " + index_flight_leg + "\n");
//                    }
//                    // update the flight instance
//                    sqlQuery = GenerateSQL.updateSeatNumMinusOne(column_available_seat, index_flight_instance, str_depart_date);
//                    rs = dbConnection.executeQuery(sqlQuery);
//                    if (rs.next()) {
//                        int available_seat = rs.getInt(1);
//                        System.out.println("Available seat: " + available_seat + "\n");
//                    }
//                    dbConnection.commit();
//                } else {
//                    System.out.println("Customer " + customer_id + " cannot make reservation because of " + class_type[idx_class] + " seat num = 0");
//                }
//            } catch (SQLException e) {
//                e.printStackTrace();
//                System.err.println("Error creating reservation");
//                dbConnection.rollback();
//            } finally {
//                dbConnection.enableAutoCommit();
//            }
//        }
//    }

    public void create_reservation(int flight_Id, String depart_date, String trip_type) {

        int[] available_seats = new int[2];
        int[] total_seats = new int[2];

        ResultSet rs = getAvailableSeats(flight_Id, depart_date);
        try {
            while (rs.next()) {
                if (rs.getString(1).equals("business"))
                    available_seats[0] = rs.getInt(2);
                else
                    available_seats[1] = rs.getInt(2);
            }
        } catch (SQLException e) {
            System.err.println("Error retrieving available seats count");
            return;
        }

        if ((available_seats[0] + available_seats[1]) == 0) {
            System.out.println("Customer " + customerId + " cannot make reservation.\nThe flight is full.");
            return;
        }

        int idx_class = simulate_class_selection();
        if (available_seats[idx_class] == 0) {
            System.out.println("Customer " + customerId + " cannot make reservation.\n" +
                    "All " + class_type[idx_class] + " class seats in the flight are full.");
            return;
        }

        rs = getTotalSeats(flight_Id, depart_date);
        try {
            while (rs.next()) {
                if (rs.getString(1).equals("business"))
                    total_seats[0] = rs.getInt(2);
                else
                    total_seats[1] = rs.getInt(2);
            }
        } catch (SQLException e) {
            System.err.println("Error retrieving total seats count");
            return;
        }

        // select seat number
        List availableSeatNumbers = getAvailableSeatNumbers(flight_Id, depart_date, idx_class);

        if (availableSeatNumbers.isEmpty()) {
            System.out.println("Customer " + customerId + " cannot make reservation.\n" +
                    "All " + class_type[idx_class] + " class seats in the flight are full.");
            return;
        }

        int seat_num = simulate_seat_selection(idx_class, total_seats, availableSeatNumbers);

        double price = getPrice(flight_Id, depart_date, idx_class);

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // Reservation Date & Time
        Date date_resv = new Date();
        String str_resv_date_time = dateFormat.format(date_resv);
        String[] str_array = str_resv_date_time.split(" ");
        String str_resv_date = str_array[0];
        String str_resv_time = str_array[1];

        String str_pay_date = str_resv_date;
        String str_pay_time = str_resv_time;

        try {
            dbConnection.disableAutoCommit();

            // insert the reservation
            int index_resv = addReservation(str_resv_date, str_resv_time, price, str_pay_date, str_pay_time, trip_type);

            // update the flight_leg
            int flight_leg = addFlightLeg(flight_Id, index_resv, depart_date);

            if (!checkSeatNumberAvailable(flight_Id, depart_date, seat_num)) {
                System.out.println("Customer " + customerId + " cannot make reservation.\n" +
                        "Seat number " + seat_num + " is assigned to another customer.");
                dbConnection.rollback();
            }
            else if (reserveSeatNumber(flight_leg, flight_Id, depart_date, seat_num))
                dbConnection.commit();
            else
                dbConnection.rollback();

        } catch (SQLException e) {
            e.printStackTrace();
            System.err.println("Error creating reservation");
            dbConnection.rollback();
        } catch (Exception e) {
            dbConnection.rollback();
        } finally {
            dbConnection.enableAutoCommit();
        }

    }

//    private void initial() {
//        get_total_fight_instance();
//    }

//    private void get_total_fight_instance() {
//        String sqlQuery = GenerateSQL.getTotalCount("flight_instance");
//        ResultSet rs = dbConnection.executeQuery(sqlQuery);
//        try {
//            if(rs.next()){
//                total_flight_instance = rs.getInt(1);
//                System.out.println("The total row in flight_instance is " + total_flight_instance);
//            }
//        } catch (SQLException e) {
//            System.err.println("Error getting total flight instance");
//            System.exit(0);
//        }
//    }

    public int simulate_class_selection(){
        Random ran = new Random();
        int seed = ran.nextInt(10);
        return (seed < 2) ? 0 : 1;
    }

//    public int simulate_flight_selection(){
//        Random ran = new Random();
//        return ran.nextInt(total_flight_instance) + 1;
//    }
//
//    public String get_column_price(int idx){
//        return idx==0? "price_bus":"price_eco";
//    }
//
//    public String get_column_available_seat(int idx) {
//        return idx == 0 ? "available_bus_seat_num" : "available_eco_seat_num";
//    }
//
//    public ResultSet get_row_fromDB(String table_name, String key_name, int key_value) {
//        String sqlQuery = GenerateSQL.getRow(table_name, key_name, key_value);
//        ResultSet rs = dbConnection.executeQuery(sqlQuery);
//        return rs;
//    }
//
//    public boolean get_insert_flag(int idx) {
//        int available_seat = (idx == 0 ? available_business_seats : available_economy_seats);
//        if (available_seat > 0)
//            return true;
//        else
//            return false;
//    }

    public int simulate_customer_selection() {
        Random ran = new Random();
        return ran.nextInt(1000) + 1;
    }

    public int simulate_seat_selection(int idx_class, int[] total_seats, List availableSeatNumbers) {
        int seat_nr = 0;
        Random ran = new Random();
        while (!availableSeatNumbers.contains(seat_nr)) {
            seat_nr = (idx_class == 0) ? ran.nextInt(total_seats[0]) + 1 : ran.nextInt(total_seats[1]) + total_seats[0] + 1;
        }

        //System.out.println("Selecting seat number " + seat_nr);
        return seat_nr;
    }

    public List getAvailableSeatNumbers(int flight_id, String depart_date, int idx_class) {
        String sqlQuery = GenerateSQL.getAvailableSeatNumbers(flight_id, depart_date, idx_class);
        ResultSet rs = dbConnection.executeQuery(sqlQuery);
        List availableSeats = new ArrayList<>();
        try {
            while (rs.next()) {
                availableSeats.add(rs.getInt(1));
            }
        } catch (SQLException e) {
            System.err.println("Error getting available seats");
        }
        return availableSeats;
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

    public int addFlightLeg(int flight_Id, int index_resv, String str_depart_date) throws SQLException {
        String sqlQuery = GenerateSQL.insertFlightLeg(flight_Id, index_resv, str_depart_date);
        ResultSet rs = dbConnection.executeQuery(sqlQuery);
        int index_flight_leg = -1;
        if (rs.next()) {
            index_flight_leg = rs.getInt(1);
            System.out.println("Flight Leg Number: " + index_flight_leg + "\n");
        }
        return index_flight_leg;
    }

//    public void updateFlightInstance(String column_available_seat, int flight_Id, String str_depart_date) throws SQLException {
//        String sqlQuery = GenerateSQL.updateSeatNumMinusOne(column_available_seat, flight_Id, str_depart_date);
//        ResultSet rs = dbConnection.executeQuery(sqlQuery);
//        if (rs.next()) {
//            int available_seats = rs.getInt(1);
//            System.out.println("Available seats: " + available_seats + "\n");
//        }
//    }

//    public ResultSet getFlightCapacity(int aircraft_id) {
//        String sqlQuery = GenerateSQL.getFlightCapacity(aircraft_id);
//        ResultSet rs = dbConnection.executeQuery(sqlQuery);
//        return rs;
//    }
//
//    public ResultSet getFlightInstance(int flight_id, String depart_date) {
//        String sqlQuery = GenerateSQL.getPrice(flight_id, depart_date);
//        ResultSet rs = dbConnection.executeQuery(sqlQuery);
//        System.out.println("Getting flight instance...");
//        return rs;
//    }

    public ResultSet getAvailableSeats(int flight_id, String depart_date) {
        String sqlQuery = GenerateSQL.getAvailableSeats(flight_id, depart_date);
        ResultSet rs = dbConnection.executeQuery(sqlQuery);
        //System.out.println("Getting available seats ...");

        return rs;
    }

    public ResultSet getTotalSeats(int flight_id, String depart_date) {
        String sqlQuery = GenerateSQL.getTotalSeats(flight_id, depart_date);
        ResultSet rs = dbConnection.executeQuery(sqlQuery);
        //System.out.println("Getting total seats ...");

        return rs;
    }

    public boolean checkSeatNumberAvailable(int flight_id, String depart_date, int seat_nr) {
        String sqlQuery = GenerateSQL.checkSeatNumberAvailable(flight_id, depart_date, seat_nr);
        ResultSet rs = dbConnection.executeQuery(sqlQuery);
        //System.out.println("Checking seat number available ...");

        try {
            if (rs.next()) {
                return (rs.getInt(1) == 0) ? true : false;
            }
        } catch (SQLException e) {
            System.err.println("Error checking availability of seat number");
            return false;
        }
        return false;
    }

    public void addRowstoSeats() throws SQLException{
        String sqlQuery = String.format("SELECT * FROM flight_instance");
        ResultSet rs = dbConnection.executeQuery(sqlQuery);
        DateFormat dateFormat1 = new SimpleDateFormat("yyyy-MM-dd");
        while (rs.next()){
            int flight_id = rs.getInt(1);
            java.sql.Date depart_date = rs.getDate(2);
            String str_depart_date = dateFormat1.format(depart_date);
            int aircraft_id =rs.getInt(10);
            sqlQuery = String.format("SELECT business_capacity, economy_capacity from aircraft where aircraft_id=%d", aircraft_id);
            ResultSet rs_sub = dbConnection.executeQuery(sqlQuery);
            int bus_capacity = 0;
            int eco_capacity = 0;
            if (rs_sub.next()){
                bus_capacity = rs_sub.getInt(1);
                eco_capacity = rs_sub.getInt(2);
            }
            System.out.println("Bus: " + bus_capacity + " Eco: " + eco_capacity);
            for (int i = 1; i <= bus_capacity; i++){
                sqlQuery = String.format("INSERT INTO seats (flight_id, depart_date, seat_id, class)\n" +
                        "values(%d, \'%s\', %d, \'business\')\n", flight_id, depart_date, i);
                dbConnection.execute(sqlQuery);
            }
            for (int i = 1; i <= eco_capacity; i++){
                sqlQuery = String.format("INSERT INTO seats (flight_id, depart_date, seat_id, class)\n" +
                        "values(%d, \'%s\', %d, \'economy\')\n", flight_id, depart_date, i + bus_capacity);
                dbConnection.execute(sqlQuery);
            }
        }
    }

    public double getPrice(int flight_id, String depart_date, int idx_class) {
        String sqlQuery = GenerateSQL.getPrice(flight_id, depart_date);
        ResultSet rs = dbConnection.executeQuery(sqlQuery);
        //System.out.println("Retrieving ticket price ...");

        try {
            if (rs.next()) {
                return rs.getDouble(idx_class + 1);
            }
        } catch (SQLException e) {
            System.err.println("Error retrieving ticket price");
        }

        return 0;
    }

    public boolean reserveSeatNumber(int flight_leg_id, int flight_id, String depart_date, int seat_nr) {
        System.out.println("Reserving seat number " + seat_nr);
        String sqlQuery = GenerateSQL.updateSeats(flight_leg_id, flight_id, depart_date, seat_nr);
        return dbConnection.execute(sqlQuery);
    }

}
