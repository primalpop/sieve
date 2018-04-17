package edu.uci.ics.tippers.db;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import org.apache.commons.dbutils.DbUtils;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.*;

/**
 * Created by cygnus on 10/29/17.
 */
public class MySQLQueryManager {

    private static final Connection connection = MySQLConnectionManager.getInstance().getConnection();


    private long timeout = 25000;


    public Duration runWithThread(String query) {

        Statement statement = null;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Duration> future = null;
        try {
            statement = connection.createStatement();
            QueryExecutor queryExecutor = new QueryExecutor(statement, query);
            future = executor.submit(queryExecutor);
            Duration timeTaken = future.get(timeout, TimeUnit.MILLISECONDS);
            executor.shutdown();
            return timeTaken;
        } catch (SQLException | InterruptedException | ExecutionException ex) {
            cancelStatement(statement, ex);
            throw new PolicyEngineException("Failed to query the database. " + ex);
        } catch (TimeoutException ex) {
            cancelStatement(statement, ex);
            future.cancel(true);
            return PolicyConstants.MAX_DURATION;
        } finally {
            DbUtils.closeQuietly(statement);
            executor.shutdownNow();
        }
    }

    private void cancelStatement(Statement statement, Exception ex) {
        System.out.println("Cancelling the current query statement. Timeout occurred" + ex);
        try {
            statement.cancel();
        } catch (SQLException exception) {
            throw new PolicyEngineException("Calling cancel() on the Statement issued exception. Details are: " + exception);
        }
    }

    private class QueryExecutor implements  Callable<Duration>{

        Statement statement;
        String query;

        public QueryExecutor(Statement statement, String query) {
            this.statement = statement;
            this.query = query;
        }

        @Override
        public Duration call() throws Exception {
            try {
                Instant start = Instant.now();
                ResultSet rs = statement.executeQuery(query);
                rs.close();
                Instant end = Instant.now();
                return Duration.between(start, end);
            } catch (SQLException e) {
                cancelStatement(statement, e);
                e.printStackTrace();
                throw new PolicyEngineException("Error Running Query");
            }
        }
    }


    /**
     * Compute the cost by execution time of the query
     * @param predicates
     * @return
     * @throws PolicyEngineException
     */

    public Duration runTimedQuery(String predicates) throws PolicyEngineException {
        try {
            return runWithThread(PolicyConstants.SELECT_ALL_SEMANTIC_OBSERVATIONS + " WHERE " + predicates);
        } catch (Exception e) {
            e.printStackTrace();
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
