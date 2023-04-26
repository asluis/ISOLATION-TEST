import java.sql.*;

public class IsolationTest{
    // Edit these fields to make the mysql connection
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
        // first phase
        System.out.println("\n-------Executing Transactions as READ COMMITTED-------");
        openConnectionMysql();

        //second phase
        System.out.println("\n-------Executing Transactions as SERIALIZABLE-------");
    }


    private void openConnectionMysql() {
        try {
            String url = MYSQL_PATH + DB;
            System.out.println("Open first connection.");
            conn1 = DriverManager.getConnection(url, USER, PW);
            System.out.println("Open second connection");
            conn2 = DriverManager.getConnection(url, USER, PW);
            System.out.println("Finished\n");
        }catch (SQLException e){
            System.out.println("SQLException: " + e.getMessage());
            System.out.println("SQLState: " + e.getSQLState());
            System.out.println("VendorError: " + e.getErrorCode());
        }
    }

    private void createTable(Connection conn){
        try{
            stmt1 = conn.createStatement();
            stmt1.executeUpdate("DROP TABLE IF EXISTS " + TABLE_NAME + ";");
            String sql = "CREATE TABLE " + TABLE_NAME + "(data int);";
            stmt1.executeUpdate(sql);

            // TODO: Insert statement(s)
            
            System.out.println("Finished\n");

        }
        catch (SQLException e){
            System.out.println("SQLException: " + e.getMessage());
            System.out.println("SQLState: " + e.getSQLState());
            System.out.println("VendorError: " + e.getErrorCode());
        }
    }

    private void closeConnectionMysql(){
        try{
            System.out.println("Closing connections.");
            conn1.close();
            conn2.close();
            System.out.println("Finished\n");
        }
        catch(SQLException e){
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