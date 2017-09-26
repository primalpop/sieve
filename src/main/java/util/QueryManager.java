package util;

import au.com.bytecode.opencsv.CSVWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;

/**
 * Created by cygnus on 7/7/17.
 *
 * Includes Q1 to Q5 from experiments
 */
public class QueryManager {


    private static QueryManager instance = null;

    protected QueryManager(){

    }

    public static QueryManager getInstance(){
        if(instance == null){
            instance = new QueryManager();
        }
        return instance;
    }

    public String [] queryList = {
            "SELECT count(*) FROM SEMANTIC_OBSERVATION where ",
            "Select from SEMANTIC_OBSERVATION where user_id = 2564 and ",
            "Select * from SEMANTIC_OBSERVATION where location = 2028",
            "Select user from SEMANTIC_OBSERVATION where timeStamp between " +
                    "\"0003-07-17 02:13:00\" and \"0003-08-17 18:06:00\" and ",
            "Select so.user_id from SEMANTIC_OBSERVATION as so, " +
                    "INFRASTRUCTURE as i, USER as u where so.user_id = u.user_id and " +
                    "i.name = so.location and u.group_name = \"ISG\" and i.type = \"lab\" and "
    };

    private Connection getConnection() throws Exception {
        String url = "jdbc:mysql://tippersweb.ics.uci.edu:3306/primal_tippers";
        String username = "test";
        String password = "test";
        return DriverManager.getConnection(url, username, password);
    }

    private void executeQuery(int queryIndex){
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(queryList[queryIndex]);
            writeResultsToCSV(rs, queryIndex);
            while (rs.next()) {
                System.out.println(rs.getString("id") + " " + rs.getString("location") + " "
                        + rs.getString("timeStamp") + " " +  rs.getString("user_id"));
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
        } finally {
            try {
                rs.close();
                stmt.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void writeResultsToCSV(ResultSet rs, int queryIndex){
        Boolean includeHeaders = false;
        CSVWriter writer = null;
        try {
            writer = new CSVWriter(new FileWriter("query" + queryIndex + ".csv"), '\t');
            writer.writeAll(rs, includeHeaders);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
