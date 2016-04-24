import java.sql.*;
import java.util.Properties;

// Connect to the Postgres DB and execute SQL queries
public class DbConnection {

    private Connection conn;

    public boolean connect() {
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
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        } catch (SQLException e) {
            System.err.println("Could not connect to database.");
            return false;
        }
        return true;
    }

    public ResultSet executeQuery(String sqlStatement) {
        Statement st = null;
        try {
            st = conn.createStatement();
            return st.executeQuery(sqlStatement);
        } catch (SQLException e) {
            System.err.println("Could not execute statement: \n" + sqlStatement);
            return null;
        }
    }

    public boolean execute(String sqlStatement) {
        Statement st = null;
        try {
            st = conn.createStatement();
            st.execute(sqlStatement);
            return true;
        } catch (SQLException e) {
            System.err.println("Could not execute statement: \n" + sqlStatement);
            return false;
        }
    }

    public void closeConnection() {
        try {
            conn.close();
        } catch (SQLException e) {
            System.err.println("Could not close database connection");
        }
    }

    public void disableAutoCommit() {
        try {
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            System.err.println("Error disabling auto commit");
        }
    }

    public void enableAutoCommit() {
        try {
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            System.err.println("Error enabling auto commit");
        }
    }

    public void commit() {
        try {
            conn.commit();
        } catch (SQLException e) {
            System.err.println("Error on commit");
        }
    }

    public void rollback() {
        try {
            conn.rollback();
        } catch (SQLException e) {
            System.err.println("Error on rollback");
        }
    }
}

