import java.sql.*;
import java.util.Properties;

// Connect to the Postgres DB and execute SQL queries
public class DbConnection {

    private static Connection conn;

    public static boolean connect() {
        // Load JDBC driver
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.err.println("Could not find the JDBC driver class.");
            return false;
        }

        // Create property object to hold username & password
        Properties myProp = new Properties();
        myProp.put("user", "team12");
        myProp.put("password", "DBproject2");

        try {
            conn = DriverManager.getConnection(
                    "jdbc:postgresql://129.7.243.243:5432/team12", myProp);
        } catch (SQLException e) {
            System.err.println("Could not connect to database.");
            return false;
        }
        return true;
    }

    public static ResultSet executeQuery(String sqlStatement) {
        Statement st = null;
        try {
            st = conn.createStatement();
            return st.executeQuery(sqlStatement);
        } catch (SQLException e) {
            System.err.println("Could not create statement: \n" + sqlStatement);
            return null;
        }
    }

    public static boolean execute(String sqlStatement) {
        Statement st = null;
        try {
            st = conn.createStatement();
            st.execute(sqlStatement);
            return true;
        } catch (SQLException e) {
            System.err.println("Could not create statement: \n" + sqlStatement);
            return false;
        }
    }

    public static void closeConnection() {
        try {
            conn.close();
        } catch (SQLException e) {
            System.err.println("Could not close database connection");
        }
    }

    public static void disableAutoCommit() {
        try {
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            System.err.println("Error disabling auto commit");
        }
    }

    public static void enableAutoCommit() {
        try {
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            System.err.println("Error enabling auto commit");
        }
    }

    public static void commit() {
        try {
            conn.commit();
        } catch (SQLException e) {
            System.err.println("Error on commit");
        }
    }

    public static void rollback() {
        try {
            conn.rollback();
        } catch (SQLException e) {
            System.err.println("Error on rollback");
        }
    }
}

