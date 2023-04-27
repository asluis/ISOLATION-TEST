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
    private Statement stmt1;
    private Statement stmt2;
    private PreparedStatement prepStmt1;
    private PreparedStatement prepStmt2;

    public IsolationTest() {

    }

    public void executeProgram(){
        try {
            System.out.println("\n-------Executing Transactions as READ COMMITTED-------");
            openConnection();
            createTable(conn1);
            conn1.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

            // Start transactions for both connections
            conn1.setAutoCommit(false);
            conn2.setAutoCommit(false);

            // Insert a row into the UNREPEATABLE table using conn1
            String sql = "INSERT INTO UNREPEATABLE (data) VALUES (1)";
            conn1.prepareStatement(sql).executeUpdate();
            System.out.println("conn1: inserted value 1");

            // Read the inserted row three times using conn2
            String selectSql = "SELECT data FROM UNREPEATABLE";
            rs = conn2.prepareStatement(selectSql).executeQuery();
            while (rs.next()) {
                int value = rs.getInt("data");
                System.out.println("conn2: read value " + value);
            }
            rs.close();

            // Update the row to have a new value using conn1
            String updateSql = "UPDATE UNREPEATABLE SET data = 2 WHERE data = 1";
            conn1.prepareStatement(updateSql).executeUpdate();
            System.out.println("conn1: updated value to 2");

            // Read the updated row three times using conn2
            rs = conn2.prepareStatement(selectSql).executeQuery();
            while (rs.next()) {
                int value = rs.getInt("data");
                System.out.println("conn2: read value " + value);
            }
            rs.close();

            // Delete the row using conn1
            String deleteSql = "DELETE FROM UNREPEATABLE WHERE data = 2";
            conn1.prepareStatement(deleteSql).executeUpdate();
            System.out.println("conn1: deleted row");

            // Commit transactions for both connections
            conn1.commit();
            conn2.commit();
            closeConnection();

            System.out.println("\n-------Executing Transactions as SERIALIZABLE-------");
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