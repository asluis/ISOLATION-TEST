import java.sql.*;

public class IsolationTest{
    
    // Modify USER, PW, and MYSQL_PATH to connect to server
    private final String USER = "cs157b";
    private final String PW = "cs157b";
    final static String MYSQL_PATH = "jdbc:mysql://localhost:3306/"; 


    private final String DB = "ISOLATION_TEST";
    private final String TABLE_NAME = "UNREPEATABLE";

    private Connection conn1;
    private Connection conn2;
    private ResultSet rs;

    public IsolationTest() {

    }

    public void executeProgram(){
        try {
            System.out.println("\n-------Executing Transactions as READ COMMITTED-------");
            openConnection();
            createTable(conn1);
            conn2.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

            // Start transactions for both connections
             conn1.setAutoCommit(false);
             conn2.setAutoCommit(false);

            // Insert a row into the UNREPEATABLE table using conn1
            String sql = "INSERT INTO UNREPEATABLE (data) VALUES (1)";
            conn1.prepareStatement(sql).executeUpdate();
            System.out.println("conn1: inserted value 1");

            // Read the inserted row three times using conn2
            readRow(conn2);

            // Update the row to have a new value using conn1
            String updateSql = "UPDATE UNREPEATABLE SET data = 2 WHERE data = 1";
            conn1.prepareStatement(updateSql).executeUpdate();
            System.out.println("conn1: updated value to 2");

            // Read the updated row three times using conn2
            readRow(conn2);

            // Delete the row using conn1
            String updateSql2 = "UPDATE UNREPEATABLE SET data = 3 WHERE data = 2";
            conn1.prepareStatement(updateSql2).executeUpdate();
            System.out.println("conn1: updated value to 3");

            // Commit transactions for both connections
            conn1.commit();
            conn2.commit();

            // Read the updated row three times using conn2
            readRow(conn2);
            closeConnection(); 

            System.out.println("\n-------Executing Transactions as SERIALIZABLE-------");
            openConnection();
            // Set isolation levels for both connections
            //conn1.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            conn2.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
 
            // Start transactions for both connections
            conn1.setAutoCommit(false);
            conn2.setAutoCommit(false);

            // Insert a row into the UNREPEATABLE table using conn1
            String insertSql = "INSERT INTO UNREPEATABLE (data) VALUES (1)";
            conn1.prepareStatement(insertSql).executeUpdate();
            System.out.println("conn1: inserted value 1");

            // Read the inserted row three times using conn2
            readRow(conn2);

            // Update the row to have a new value using conn2
            updateSql = "UPDATE UNREPEATABLE SET data = 2 WHERE data = 1";
            Statement stmt2 = conn2.createStatement();
            System.out.println("conn2: trying to acquire lock for update");
            stmt2.executeUpdate(updateSql);
            System.out.println("conn2: acquired lock and updated value to 2");

            // Read the updated row using conn1
            readRow(conn1);

            // Try to update the row again using conn2, which will timeout waiting for the lock
            System.out.println("conn2: trying to acquire lock for second update");
            boolean timedOut = stmt2.execute(updateSql);
            if (timedOut) {
                System.out.println("conn2: timed out waiting for lock");
            }
            stmt2.close();

            // Read the updated row using conn1
            readRow(conn1);

            // Commit transactions for both connections
            conn1.commit();
            conn2.commit();

            // Read the updated row using conn1
            readRow(conn1);

            closeConnection(); 

        } catch (SQLException e){
            System.out.println("SQLException: " + e.getMessage());
            System.out.println("SQLState: " + e.getSQLState());
            System.out.println("VendorError: " + e.getErrorCode());
        }
    }


    private void openConnection() {
        try {
            String url = MYSQL_PATH + DB;
            conn1 = DriverManager.getConnection(url, USER, PW);
            System.out.println("Opened first connection successfully");
            conn2 = DriverManager.getConnection(url, USER, PW);
            System.out.println("Opened second connection successfully \n");

        }catch (SQLException e){
            System.out.println("SQLException: " + e.getMessage());
            System.out.println("SQLState: " + e.getSQLState());
            System.out.println("VendorError: " + e.getErrorCode());
        }
    }

    private void readRowFromConn2() {
        try {
            String selectSql = "SELECT * FROM UNREPEATABLE";
            rs = conn2.prepareStatement(selectSql).executeQuery();
            while (rs.next()) {
                int value = rs.getInt("data");
                System.out.println("conn2: read value " + value);
            }
            rs.close();
        } catch (SQLException e){
            System.out.println("SQLException: " + e.getMessage());
            System.out.println("SQLState: " + e.getSQLState());
            System.out.println("VendorError: " + e.getErrorCode());
        }
        
    }

    private void readRowFromConn1() {
        try {
            String selectSql = "SELECT * FROM UNREPEATABLE";
            rs = conn1.prepareStatement(selectSql).executeQuery();
            while (rs.next()) {
                int value = rs.getInt("data");
                System.out.println("conn2: read value " + value);
            }
            rs.close();
        } catch (SQLException e){
            System.out.println("SQLException: " + e.getMessage());
            System.out.println("SQLState: " + e.getSQLState());
            System.out.println("VendorError: " + e.getErrorCode());
        }
        
    }

    private void readRow(Connection conn) {
        try {
            String selectSql = "SELECT * FROM UNREPEATABLE";
            rs = conn.prepareStatement(selectSql).executeQuery();
            while (rs.next()) {
                int value = rs.getInt("data");
                System.out.println("conn2: read value " + value);
            }
            rs.close();
        } catch (SQLException e){
            System.out.println("SQLException: " + e.getMessage());
            System.out.println("SQLState: " + e.getSQLState());
            System.out.println("VendorError: " + e.getErrorCode());
        }
    }

    private void closeConnection() {
        try{
            conn1.close();
            conn2.close();
            System.out.println("Closed connections successfully\n");
        }
        catch(SQLException e){
            System.out.println("SQLException: " + e.getMessage());
            System.out.println("SQLState: " + e.getSQLState());
            System.out.println("VendorError: " + e.getErrorCode());
        }
    }

    private void createTable(Connection conn){
        try{
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("DROP TABLE IF EXISTS " + TABLE_NAME + ";");
            stmt.executeUpdate("CREATE TABLE " + TABLE_NAME + "(data INTEGER);");
        }
        catch (SQLException e){
            System.out.println("SQLException: " + e.getMessage());
            System.out.println("SQLState: " + e.getSQLState());
            System.out.println("VendorError: " + e.getErrorCode());
        }
    }

    public static void main(String[] args){
        try {
            IsolationTest test = new IsolationTest();
            test.executeProgram();
        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
            //System.out.println("SQLState: " + e.getSQLState());
            //System.out.println("VendorError: " + e.getErrorCode());
        }
        
    }
}