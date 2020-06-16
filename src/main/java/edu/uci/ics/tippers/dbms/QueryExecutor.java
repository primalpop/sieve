package edu.uci.ics.tippers.dbms;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import org.apache.commons.dbutils.DbUtils;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.*;

public class QueryExecutor {

    private long timeout = 0;

    private final Connection connection;

    public QueryExecutor(Connection connection, long timeout){
        this.connection = connection;
        this.timeout = timeout + PolicyConstants.MAX_DURATION.toMillis();
    }

    public QueryResult runWithThread(String query, QueryResult queryResult) {

        Statement statement = null;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<QueryResult> future = null;
        try {
            statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            Executor queryExecutor = new Executor(statement, query, queryResult);
            future = executor.submit(queryExecutor);
            queryResult = future.get(timeout, TimeUnit.MILLISECONDS);
            executor.shutdown();
            return queryResult;
        } catch (SQLException | InterruptedException | ExecutionException ex) {
            cancelStatement(statement, ex);
            ex.printStackTrace();
            throw new PolicyEngineException("Failed to query the database. " + ex);
        } catch (TimeoutException ex) {
            cancelStatement(statement, ex);
            future.cancel(true);
            queryResult.setTimeTaken(PolicyConstants.MAX_DURATION);
            return queryResult;
        } finally {
            DbUtils.closeQuietly(statement);
            executor.shutdownNow();
        }
    }

    private void cancelStatement(Statement statement, Exception ex) {
        System.out.println("Cancelling the current query statement. Timeout occurred");
        try {
            statement.cancel();
        } catch (SQLException exception) {
            throw new PolicyEngineException("Calling cancel() on the Statement issued exception. Details are: " + exception);
        }
    }

    private class Executor implements  Callable<QueryResult>{

        Statement statement;
        String query;
        QueryResult queryResult;

        public Executor(Statement statement, String query, QueryResult queryResult) {
            this.statement = statement;
            this.query = query;
            this.queryResult = queryResult;
        }

        @Override
        public QueryResult call() throws Exception {
            try {
                Instant start = Instant.now();
                ResultSet rs = statement.executeQuery(query);
                Instant end = Instant.now();
                if(queryResult.getResultsCheck())
                    queryResult.setQueryResult(rs);
                int rowcount = 0;
                if (hasColumn(rs, "total")){
                    rs.next();
                    rowcount = rs.getInt(1);
                }
                else if (rs.last()) {
                    rowcount = rs.getRow();
                    rs.beforeFirst();
                }
                if(queryResult.getPathName() != null && queryResult.getFileName() != null){
                    queryResult.writeResultsToFile(rs);
                }
                queryResult.setResultCount(rowcount);
                queryResult.setTimeTaken(Duration.between(start, end));
                return queryResult;
            } catch (SQLException e) {
                System.out.println("Exception raised by : " + query);
                cancelStatement(statement, e);
                e.printStackTrace();
                throw new PolicyEngineException("Error Running Query");
            }
        }

        public boolean hasColumn(ResultSet rs, String columnName) throws SQLException {
            ResultSetMetaData rsmd = rs.getMetaData();
            int columns = rsmd.getColumnCount();
            for (int x = 1; x <= columns; x++) {
                if (columnName.equals(rsmd.getColumnName(x))) {
                    return true;
                }
            }
            return false;
        }
    }

}
