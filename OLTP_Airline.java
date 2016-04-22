import com.sun.org.apache.xerces.internal.xs.StringList;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class OLTP_Airline {

    public static void main (String[] args) throws SQLException{

        // Connect to database
        if (!DbConnection.connect()) {
            System.out.println("Cannot connected to database.");
            return;
        }
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
        String str_pay_date_time = dateFormat.format(date_resv);
        str_array = str_pay_date_time.split(" ");
        String str_pay_date = str_array[0];
        String str_pay_time = str_array[1];

        String sqlQuery;

        sqlQuery = "SELECT COUNT(*) FROM flight_instance";
        ResultSet rs = DbConnection.executeQuery(sqlQuery);

        if(rs.next()){
            int total_flight_instance = rs.getInt(1);
            System.out.println("The total row in flight_instance is " + total_flight_instance);
        }

        sqlQuery = "INSERT INTO reservation (resv_date, resv_time, resv_status, pay_amount, pay_method, pay_date, pay_time, customer_id, resv_type)\n"
                + "VALUES (" + str_resv_date + " " + str_resv_time + " " + "confirmed" + " " + "100" + " "+ "cash" + " " + str_pay_date + " " + str_pay_time + " " + customer_id + " " + "single)";


        System.out.println(sqlQuery);
    }
}
