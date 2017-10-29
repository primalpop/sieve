package db;

import common.PolicyEngineException;
import model.policy.BooleanCondition;

import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;

/**
 * Created by cygnus on 10/29/17.
 */
public class MySQLQueryManager {

    private Connection connection;

    public MySQLQueryManager(){
        connection = MySQLConnectionManager.getInstance().getConnection();
    }

    /**
     * Compute the cost by execution time of the query
     * @param query
     * @return
     * @throws PolicyEngineException
     */

    public Duration runTimedQuery(String query) throws PolicyEngineException {
        try {
            PreparedStatement stmt = connection.prepareStatement(query);
            Instant start = Instant.now();
            ResultSet rs = stmt.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            rs.close();
            Instant end = Instant.now();
            return Duration.between(start, end);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new PolicyEngineException("Error Running Query");
        }
    }


    /**
     * Compute the number of false positives by counting the number of results
     * @param predicates
     */
    public int runCountingQuery(String predicates){
        int count = 0;
        String query = "Select count(*) from SEMANTIC_OBSERVATION where " + predicates;
        try{
            PreparedStatement stmt = connection.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            while(rs.next())
                count=rs.getInt(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return count;
    }
}
