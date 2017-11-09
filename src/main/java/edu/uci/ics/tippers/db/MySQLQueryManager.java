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

    private Connection connection;

    public MySQLQueryManager(){
        connection = MySQLConnectionManager.getInstance().getConnection();
    }

    /**
     * Compute the cost by execution time of the query
     * @param predicates
     * @return
     * @throws PolicyEngineException
     */

    public long runTimedQuery(String predicates) throws PolicyEngineException {
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

    /**
     * Compute the number of false positives by counting the number of results
     * @param predicates
     */
    public long runCountingQuery(String predicates){
        long fullCount = 0, count = 0;
        String query = PolicyConstants.SELECT_COUNT_STAR_SEMANTIC_OBSERVATIONS + " where " +  predicates;
        String allCount = PolicyConstants.SELECT_COUNT_STAR_SEMANTIC_OBSERVATIONS;
        try{
            PreparedStatement stmtQ = connection.prepareStatement(query);
            ResultSet rsQ = stmtQ.executeQuery();
            while(rsQ.next())
                count=rsQ.getInt(1);
            PreparedStatement stmtF = connection.prepareStatement(allCount);
            ResultSet rsF = stmtF.executeQuery();
            while(rsF.next())
                count=rsF.getInt(1);

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return fullCount - count;
    }
}
