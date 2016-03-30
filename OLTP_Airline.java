
public class OLTP_Airline {

    public static void main (String[] args) {

        // Connect to database
        if (DbConnection.connect())
            System.out.println("Connected to database.");

        DbConnection.closeConnection();
    }
}
