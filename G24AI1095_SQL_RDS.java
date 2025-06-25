import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLonRDS {

    private Connection con;
    private String url = "mydb.c7e0mgugysjv.ap-south-1.rds.amazonaws.com:3306";
    private String uid = "admin";
    private String pw = "changememugu";

    public static void main(String[] args) {
        SQLonRDS q = new SQLonRDS();
        try {
            q.connect();
            // q.create();  // Creates tables
            // q.insert();  // Inserts all data
            // q.delete();
            // q.queryOne();
            // q.queryTwo();
            // q.queryThree();
            q.close();
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void connect() throws SQLException, ClassNotFoundException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        String jdbcUrl = "jdbc:mysql://" + url + "/mydb?user=" + uid + "&password=" + pw;
        System.out.println("Connecting to database...");
        con = DriverManager.getConnection(jdbcUrl);
        System.out.println("Connection Successful!");
    }

    public void close() throws SQLException {
        if (con != null) {
            con.close();
        }
    }

    public void create() throws SQLException {
        Statement stmt = con.createStatement();

        String createCompanyTable = "CREATE TABLE IF NOT EXISTS company (" +
                "id INT PRIMARY KEY," +
                "name VARCHAR(50)," +
                "ticker CHAR(10)," +
                "annualRevenue DECIMAL(15,2)," +
                "numEmployees INT" +
                ")";

        String createStockPriceTable = "CREATE TABLE IF NOT EXISTS stockprice (" +
                "companyId INT," +
                "priceDate DATE," +
                "openPrice DECIMAL(10,2)," +
                "highPrice DECIMAL(10,2)," +
                "lowPrice DECIMAL(10,2)," +
                "closePrice DECIMAL(10,2)," +
                "volume INT," +
                "PRIMARY KEY (companyId, priceDate)," +
                "FOREIGN KEY (companyId) REFERENCES company(id)" +
                ")";

        stmt.executeUpdate(createCompanyTable);
        stmt.executeUpdate(createStockPriceTable);

        System.out.println("Tables created successfully.");
    }

    public void insert() throws SQLException {
        Statement stmt = con.createStatement();
        
        // cleaning old data 
        stmt.executeUpdate("DELETE FROM stockprice");
        stmt.executeUpdate("DELETE FROM company");


        // Insert company data (includes Handy Repair and StartUp)
        stmt.executeUpdate("INSERT INTO company VALUES " +
                "(1, 'Apple', 'AAPL', 387540000000.00, 154000)," +
                "(2, 'GameStop', 'GME', 611000000.00, 12000)," +
                "(3, 'Handy Repair', NULL, 2000000, 50)," +
                "(4, 'Microsoft', 'MSFT', 198270000000.00, 221000)," +
                "(5, 'StartUp', NULL, 50000, 3)");

        // Stock data for Apple (1)
        stmt.executeUpdate("INSERT INTO stockprice VALUES " +
                "(1, '2022-08-15', 171.52, 173.39, 171.35, 173.19, 54091700)," +
                "(1, '2022-08-16', 172.78, 173.71, 171.66, 173.03, 56377100)," +
                "(1, '2022-08-17', 172.77, 176.15, 172.57, 174.55, 79542000)," +
                "(1, '2022-08-18', 173.75, 174.90, 173.12, 174.15, 62290100)," +
                "(1, '2022-08-19', 173.03, 173.74, 171.31, 171.52, 70211500)," +
                "(1, '2022-08-22', 169.69, 169.86, 167.14, 167.57, 69026800)," +
                "(1, '2022-08-23', 167.08, 168.71, 166.65, 167.23, 54147100)," +
                "(1, '2022-08-24', 167.32, 168.11, 166.25, 167.53, 53841500)," +
                "(1, '2022-08-25', 168.78, 170.14, 168.35, 170.03, 51218200)," +
                "(1, '2022-08-26', 170.57, 171.05, 163.56, 163.62, 78823500)," +
                "(1, '2022-08-29', 161.15, 162.90, 159.82, 161.38, 73314000)," +
                "(1, '2022-08-30', 162.13, 162.56, 157.72, 158.91, 77906200)");

        // Stock data for GameStop (2)
        stmt.executeUpdate("INSERT INTO stockprice VALUES " +
                "(2, '2022-08-15', 39.75, 40.39, 38.81, 39.68, 5243100)," +
                "(2, '2022-08-16', 39.17, 45.53, 38.60, 42.19, 23602800)," +
                "(2, '2022-08-17', 42.18, 44.36, 40.41, 40.52, 9766400)," +
                "(2, '2022-08-18', 39.27, 40.07, 37.34, 37.93, 8145400)," +
                "(2, '2022-08-19', 35.18, 37.19, 34.67, 36.49, 9525600)," +
                "(2, '2022-08-22', 34.31, 36.20, 34.20, 34.50, 5798600)," +
                "(2, '2022-08-23', 34.70, 34.99, 33.45, 33.53, 4836300)," +
                "(2, '2022-08-24', 34.00, 34.94, 32.44, 32.50, 5620300)," +
                "(2, '2022-08-25', 32.84, 32.89, 31.50, 31.96, 4726300)," +
                "(2, '2022-08-26', 31.50, 32.38, 30.63, 30.94, 4289500)," +
                "(2, '2022-08-29', 30.48, 32.75, 30.38, 31.55, 4292700)," +
                "(2, '2022-08-30', 31.62, 31.87, 29.42, 29.84, 5060200)");

        // Stock data for Microsoft (4)
        stmt.executeUpdate("INSERT INTO stockprice VALUES " +
                "(4, '2022-08-15', 291.00, 294.18, 290.11, 293.47, 18085700)," +
                "(4, '2022-08-16', 291.99, 294.04, 290.42, 292.71, 18102900)," +
                "(4, '2022-08-17', 289.74, 293.35, 289.47, 291.32, 18253400)," +
                "(4, '2022-08-18', 290.19, 291.91, 289.08, 290.17, 17186200)," +
                "(4, '2022-08-19', 288.90, 289.25, 285.56, 286.15, 20557200)," +
                "(4, '2022-08-22', 282.08, 282.46, 277.22, 277.75, 25061100)," +
                "(4, '2022-08-23', 276.44, 278.86, 275.40, 276.44, 17527400)," +
                "(4, '2022-08-24', 275.41, 277.23, 275.11, 275.79, 18137000)," +
                "(4, '2022-08-25', 277.33, 279.02, 274.52, 278.85, 16583400)," +
                "(4, '2022-08-26', 279.08, 280.34, 267.98, 268.09, 27532500)," +
                "(4, '2022-08-29', 265.85, 267.40, 263.85, 265.23, 20338500)," +
                "(4, '2022-08-30', 266.67, 267.05, 260.66, 262.97, 22767100)");

        System.out.println("All company and stock data inserted successfully.");
    }
    
    public void delete() throws SQLException {
        Statement stmt = con.createStatement();

         // Delete stock prices before 2022-08-20
        int deletedByDate = stmt.executeUpdate(
            "DELETE FROM stockprice WHERE priceDate < '2022-08-20'");

        // Delete stock prices where company is GameStop
        int deletedByCompany = stmt.executeUpdate(
            "DELETE FROM stockprice WHERE companyId IN (SELECT id FROM company WHERE name = 'GameStop')");

        System.out.println("Deleted " + deletedByDate + " records older than 2022-08-20.");
        System.out.println("Deleted " + deletedByCompany + " records of GameStop.");
    }

    public void queryOne() throws SQLException {
        Statement stmt = con.createStatement();

        String sql = "SELECT name, annualRevenue, numEmployees " + 
                "FROM company " +
                "WHERE numEmployees > 10000 OR annualRevenue < 1000000 " +
                "ORDER BY name ASC";

        var result = stmt.executeQuery(sql);

        System.out.println("Query One: Companies with >10,000 employees OR revenue < $1M");
        while (result.next()) {
            String name = result.getString("name");
            double revenue = result.getDouble("annualRevenue");
            int employees = result.getInt("numEmployees");

            System.out.println("Name: " + name + ", Revenue: " + revenue + ", Employees: " + employees);
        }
    }

    public void queryTwo() throws SQLException {
        Statement stmt = con.createStatement();

        String sql = "SELECT c.name, c.ticker, " +
                "MIN(s.lowPrice) AS minLow, " +
                "MAX(s.highPrice) AS maxHigh, " +
                "AVG(s.closePrice) AS avgClose, " +
                "AVG(s.volume) AS avgVolume " +
                "FROM company c JOIN stockprice s ON c.id = s.companyId " +
                "WHERE s.priceDate BETWEEN '2022-08-22' AND '2022-08-26' " +
                "GROUP BY c.id, c.name, c.ticker " +
                "ORDER BY avgVolume DESC";

        var result = stmt.executeQuery(sql);

        System.out.println("Query Two: Company stats from Aug 22–26 (ordered by avg volume DESC)");
        while (result.next()) {
            String name = result.getString("name");
            String ticker = result.getString("ticker");
            double minLow = result.getDouble("minLow");
            double maxHigh = result.getDouble("maxHigh");
            double avgClose = result.getDouble("avgClose");
            double avgVolume = result.getDouble("avgVolume");

            System.out.println("Name: " + name + ", Ticker: " + ticker +
                ", Min Low: " + minLow + ", Max High: " + maxHigh +
                ", Avg Close: " + avgClose + ", Avg Volume: " + avgVolume);
         }
    }

    public void queryThree() throws SQLException {
        Statement stmt = con.createStatement();

        String sql = "SELECT c.name, c.ticker, s30.closePrice " +
                "FROM company c " +
                "LEFT JOIN ( " +
                "SELECT companyId, closePrice " +
                "FROM stockprice WHERE priceDate = '2022-08-30' " +
                ") s30 ON c.id = s30.companyId " +
                "LEFT JOIN ( " +
                "SELECT companyId, AVG(closePrice) AS weekAvg " +
                "FROM stockprice " +
                "WHERE priceDate BETWEEN '2022-08-15' AND '2022-08-19' " +
                "GROUP BY companyId " +
                ") sWeek ON c.id = sWeek.companyId " +
                "WHERE c.ticker IS NULL " +
                "OR (s30.closePrice IS NOT NULL AND s30.closePrice >= 0.9 * sWeek.weekAvg) " +
                "ORDER BY c.name";

        var result = stmt.executeQuery(sql);

        System.out.println("Query Three: Companies passing 10% price check or without ticker");
        while (result.next()) {
            String name = result.getString("name");
            String ticker = result.getString("ticker");
            double close = result.getDouble("closePrice");

            System.out.println("Name: " + name + ", Ticker: " + ticker + ", Close (Aug 30): " + close);
        }
    }



}

