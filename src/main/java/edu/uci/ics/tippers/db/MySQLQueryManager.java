package edu.uci.ics.tippers.db;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;

/**
 * Created by cygnus on 10/29/17.
 */
public class MySQLQueryManager {

    private static final Connection connection = MySQLConnectionManager.getInstance().getConnection();

    /**
     * Compute the cost by execution time of the query
     * @param predicates
     * @return
     * @throws PolicyEngineException
     */

    public static long runTimedQuery(String predicates) throws PolicyEngineException {
        String query = PolicyConstants.SELECT_ALL_SEMANTIC_OBSERVATIONS + " where " +  predicates;
        try {
            PreparedStatement stmt = connection.prepareStatement(query);
            Instant start = Instant.now();
            ResultSet rs = stmt.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            Instant end = Instant.now();
            return Duration.between(start, end).toMillis();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(query);
            throw new PolicyEngineException("Error Running Query");
        }
    }


    public static long runCountingQuery(String query){
        long count = 0;
        try{
            PreparedStatement stmtQ = connection.prepareStatement(query);
            ResultSet rsQ = stmtQ.executeQuery();
            while(rsQ.next())
                count =rsQ.getInt(1);
        } catch (SQLException e) {
            System.out.println(query);
            e.printStackTrace();
        }
        return count;
    }


    /**
     * Compute the number of false positives by counting the number of results
     */
    public static long computeFalsePositives(String original, String modified){
        String original_query = PolicyConstants.SELECT_COUNT_STAR_SEMANTIC_OBSERVATIONS + " where " +  original;
        String modified_query = PolicyConstants.SELECT_COUNT_STAR_SEMANTIC_OBSERVATIONS + " where " + original;
        return runCountingQuery(modified_query) - runCountingQuery(original_query);
    }
}
