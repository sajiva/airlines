import com.sun.org.apache.xerces.internal.xs.StringList;

import java.sql.*;
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

    public static void main (String[] args) throws SQLException{

        // Connect to database
        if (!DbConnection.connect()) {
            System.out.println("Cannot connected to database.");
            return;
        }
        initial();
        create_reservation_one(1, 1);
        DbConnection.closeConnection();
    }

    public static void create_reservation_one(int customer_id, int seat_num) throws SQLException{
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
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
        double price = 100;
        String str_depart_date;

        int seatnum = 0;

        int idx_class = simulate_class_selection();
        String column_price = get_column_price(idx_class);

        if(total_flight_instance>0){
            // flight id
            int index_flight_instance = simulate_flight_selection();
            // get the price available seat
            ResultSet rs = get_row_fromDB("flight_instance", index_flight_instance);
            ResultSetMetaData metaData = rs.getMetaData();
            if(rs.next()){
                java.sql.Date depart_date = rs.getDate(2);
                str_depart_date = dateFormat.format(depart_date);
                available_business_seats = rs.getInt(7);
                available_economy_seats = rs.getInt(8);
                index_aircraft = rs.getInt(12);
            }

            // insert the reservation
            sqlQuery = GenerateSQL.insertReservation(customer_id, str_resv_date, str_resv_time, price, str_pay_date, str_pay_time, "single");
            //DbConnection.execute(sqlQuery);

            // get the reservation id

            // update the flight_leg

        }
    }

    private static void initial() throws SQLException{
        get_total_fight_instance();
    }

    private static void get_total_fight_instance() throws SQLException{
        String sqlQuery = GenerateSQL.getTotalCount("flight_instance");
        ResultSet rs = DbConnection.executeQuery(sqlQuery);
        if(rs.next()){
            total_flight_instance = rs.getInt(1);
            System.out.println("The total row in flight_instance is " + total_flight_instance);
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
        return ran.nextInt(total_flight_instance);
    }

    //public static int simulate_

    public static String get_column_price(int idx){
        return idx==0? "price_bus":"price_eco";
    }

    public static double get_price_fromDB(String column_price, int index_flight_instance) throws SQLException{
        String sqlQuery = GenerateSQL.getNthRowItem("flight_instance", column_price, index_flight_instance);
        ResultSet rs = DbConnection.executeQuery(sqlQuery);
        double price = 0;

        if(rs.next()){
            price = rs.getDouble(1);
            System.out.println("The price of " + column_price + " is " + price);
        }

        return price;
    }

    public static ResultSet get_row_fromDB(String tablename, int index) throws SQLException{
        String sqlQuery = GenerateSQL.getRow(tablename, index);
        ResultSet rs = DbConnection.executeQuery(sqlQuery);
        return rs;
    }
}
