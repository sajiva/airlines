/**********************************************************************************************/
/* COSC6340: Database Systems                                                                 */
/* Project 2: ER Model and OLTP for an Airline Database                                       */
/* Project team: Sajiva Pradhan (1007766), Xiang Xu (1356333)                                 */
/**********************************************************************************************/

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class OLTP_Airline extends Thread {

    private int customerId;
    private DbConnection dbConnection;

    public OLTP_Airline() {
        dbConnection = new DbConnection(); // Create new database connection
    }

    public static void main (String[] args) {

        if (args.length != 1) {
            System.err.println("The number of argument is wrong! The syntax should be: oltp_airline nthreads=<number>");
            return;
        }

        int nthreads = 1;
        // Parse command line argument
        if (args[0].contains("nthreads=")) {
            int beginInd = args[0].indexOf("=") + 1;
            try {
                nthreads = Integer.parseInt(args[0].substring(beginInd));
            } catch (Exception e) {
                System.err.println("The command format is wrong! The syntax should be: oltp_airline nthreads=<number>");
                return;
            }
        }
        else {
            System.err.println("The command format is wrong! The syntax should be: oltp_airline nthreads=<number>");
            return;
        }

        // create threads
        OLTP_Airline[] threads = new OLTP_Airline[nthreads];

        // start the threads
        for (int i = 0; i < nthreads; i++) {
            if ((i > 0) && ((i % 40) == 0)) {
                try {
                    Thread.sleep(10000); // Sleep for 10 seconds
                } catch (InterruptedException e) {
                    System.err.println(e.getMessage());
                }
            }

            threads[i] = new OLTP_Airline();
            threads[i].start();
            System.out.println("Thread " + i + " started");
        }

        // Wait for the threads to finish
        for (int i = 0; i < threads.length; i++) {
            try {
                threads[i].join();
                System.out.println("Thread " + i + " end");
            } catch (InterruptedException e) {
                System.err.println(e.getMessage());
            }
        }
    }

    public void run() {
        if (!dbConnection.connect()) {
            return;
        }
        customerId = simulate_customer_selection();

        Map<Integer, String> trip = new HashMap<>();
        trip.put(6, "2016-05-18");
        trip.put(7, "2016-05-18");
        trip.put(8, "2016-06-08");
        create_reservation(trip);

        dbConnection.closeConnection();
    }

    // create reservation
    public void create_reservation(Map<Integer, String> trip) {

        int[] available_seats = new int[2];
        int[] total_seats = new int[2];
        int[] seat_num = new int[trip.size()];
        String [] class_type = {"business", "economy"};
        double price = 0;
        int i = 0;
        ResultSet rs;

        for (Map.Entry<Integer, String> entry : trip.entrySet()) {
            int flight_Id = entry.getKey();
            String depart_date = entry.getValue();

            // query number of available seats in the flight
            rs = getAvailableSeats(flight_Id, depart_date);
            try {
                while (rs.next()) {
                    if (rs.getString(1).equals("business"))
                        available_seats[0] = rs.getInt(2); // number of business class seats available
                    else
                        available_seats[1] = rs.getInt(2); // number of economy class seats available
                }
            } catch (SQLException e) {
                System.err.println("Error retrieving available seats count");
                return;
            }

            // if no seats available in the flight
            if ((available_seats[0] + available_seats[1]) == 0) {
                System.out.println("Customer " + customerId + " cannot make reservation.\nThe flight " + flight_Id + " is full.");
                return;
            }

            // select a class in random
            int idx_class = simulate_class_selection();
            // if no seats available for selected class
            if (available_seats[idx_class] == 0) {
                System.out.println("Customer " + customerId + " cannot make reservation.\n" +
                        "All " + class_type[idx_class] + " class seats in the flight " + flight_Id + " are full.");
                return;
            }

            // query total seats in the flight
            rs = getTotalSeats(flight_Id, depart_date);
            try {
                while (rs.next()) {
                    if (rs.getString(1).equals("business"))
                        total_seats[0] = rs.getInt(2); // total number of business class seats
                    else
                        total_seats[1] = rs.getInt(2); // total number of economy class seats
                }
            } catch (SQLException e) {
                System.err.println("Error retrieving total seats count");
                return;
            }

            // query available seat numbers in the flight for selected class
            List availableSeatNumbers = getAvailableSeatNumbers(flight_Id, depart_date, idx_class);

            // if no seats available
            if (availableSeatNumbers.isEmpty()) {
                System.out.println("Customer " + customerId + " cannot make reservation.\n" +
                        "All " + class_type[idx_class] + " class seats in the flight are full.");
                return;
            }

            // select a seat number in random
            seat_num[i++] = simulate_seat_selection(idx_class, total_seats, availableSeatNumbers);
            // retrieve price for the flight ticket
            price += getPrice(flight_Id, depart_date, idx_class);
        }

        // Reservation Date & Time
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date_resv = new Date();
        String str_resv_date_time = dateFormat.format(date_resv);
        String[] str_array = str_resv_date_time.split(" ");
        String str_resv_date = str_array[0];
        String str_resv_time = str_array[1];

        String trip_type = (trip.size() > 1) ? "multi" : "single";
        int j = 0;

        try {
            // Start the transaction
            dbConnection.disableAutoCommit();

            // add the reservation
            int index_resv = addReservation(str_resv_date, str_resv_time, price, trip_type);

            for (Map.Entry<Integer, String> entry : trip.entrySet()) { // for each flight in the trip
                int flight_Id = entry.getKey();
                String depart_date = entry.getValue();
                int seat_nr = seat_num[j++];

                // add the flight_leg
                int flight_leg = addFlightLeg(flight_Id, index_resv, depart_date);

                // check if the selected seat number is available
                if (!checkSeatNumberAvailable(flight_Id, depart_date, seat_nr)) {
                    System.out.println("Customer " + customerId + " cannot make reservation.\n" +
                            "Seat number " + seat_nr + " on flight " + flight_Id + " is assigned to another customer.");
                    dbConnection.rollback();
                    break;
                }
                // reserve the seat number
                if (!reserveSeatNumber(flight_leg, flight_Id, depart_date, seat_nr)) {
                    dbConnection.rollback();
                    break;
                }
            }
            // commit the transaction
            dbConnection.commit();

        } catch (Exception e) {
            System.err.println("Error creating reservation: " + e.getMessage());
            dbConnection.rollback();
        } finally {
            // enable autocommit
            dbConnection.enableAutoCommit();
        }
    }

    private int simulate_customer_selection() {
        Random ran = new Random();
        return ran.nextInt(1000) + 1;
    }

    private int simulate_class_selection(){
        Random ran = new Random();
        int seed = ran.nextInt(10);
        return (seed < 2) ? 0 : 1;
    }

    private int simulate_seat_selection(int idx_class, int[] total_seats, List availableSeatNumbers) {
        int seat_nr = 0;
        Random ran = new Random();
        while (!availableSeatNumbers.contains(seat_nr)) {
            seat_nr = (idx_class == 0) ? ran.nextInt(total_seats[0]) + 1 : ran.nextInt(total_seats[1]) + total_seats[0] + 1;
        }

        return seat_nr;
    }

    private ResultSet getAvailableSeats(int flight_id, String depart_date) {
        String sqlQuery = GenerateSQL.getAvailableSeats(flight_id, depart_date);
        return dbConnection.executeQuery(sqlQuery);
    }

    private ResultSet getTotalSeats(int flight_id, String depart_date) {
        String sqlQuery = GenerateSQL.getTotalSeats(flight_id, depart_date);
        return dbConnection.executeQuery(sqlQuery);
    }

    private List getAvailableSeatNumbers(int flight_id, String depart_date, int idx_class) {
        String sqlQuery = GenerateSQL.getAvailableSeatNumbers(flight_id, depart_date, idx_class);
        ResultSet rs = dbConnection.executeQuery(sqlQuery);
        List<Integer> availableSeats = new ArrayList<>();
        try {
            while (rs.next()) {
                availableSeats.add(rs.getInt(1));
            }
        } catch (SQLException e) {
            System.err.println("Error getting available seats");
        }
        return availableSeats;
    }

    private boolean checkSeatNumberAvailable(int flight_id, String depart_date, int seat_nr) {
        String sqlQuery = GenerateSQL.checkSeatNumberAvailable(flight_id, depart_date, seat_nr);
        ResultSet rs = dbConnection.executeQuery(sqlQuery);

        try {
            if (rs.next()) {
                return (rs.getInt(1) == 0);
            }
        } catch (SQLException e) {
            System.err.println("Error checking availability of seat number");
            return false;
        }
        return false;
    }

    private double getPrice(int flight_id, String depart_date, int idx_class) {
        String sqlQuery = GenerateSQL.getPrice(flight_id, depart_date);
        ResultSet rs = dbConnection.executeQuery(sqlQuery);

        try {
            if (rs.next()) {
                return rs.getDouble(idx_class + 1);
            }
        } catch (SQLException e) {
            System.err.println("Error retrieving ticket price");
        }

        return 0;
    }

    private int addReservation(String str_resv_date, String str_resv_time, double price, String trip_type) throws SQLException {
        String sqlQuery = GenerateSQL.insertReservation(customerId, str_resv_date, str_resv_time, price, trip_type);
        ResultSet rs = dbConnection.executeQuery(sqlQuery);
        int index_resv = -1;
        if (rs.next()) {
            index_resv = rs.getInt(1);
            System.out.println("Reservation Number: " + index_resv + "\n");
        }
        return index_resv;
    }

    private int addFlightLeg(int flight_Id, int index_resv, String str_depart_date) throws SQLException {
        String sqlQuery = GenerateSQL.insertFlightLeg(flight_Id, index_resv, str_depart_date);
        ResultSet rs = dbConnection.executeQuery(sqlQuery);
        int index_flight_leg = -1;
        if (rs.next()) {
            index_flight_leg = rs.getInt(1);
            System.out.println("Flight Leg Number: " + index_flight_leg + "\n");
        }
        return index_flight_leg;
    }

    private boolean reserveSeatNumber(int flight_leg_id, int flight_id, String depart_date, int seat_nr) {
        System.out.println("Reserving seat number " + seat_nr);
        String sqlQuery = GenerateSQL.updateSeats(flight_leg_id, flight_id, depart_date, seat_nr);
        return dbConnection.execute(sqlQuery);
    }

}
