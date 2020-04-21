package edu.uci.ics.tippers.db;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import org.apache.commons.dbutils.DbUtils;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by cygnus on 10/29/17.
 */
public class MySQLQueryManager {

    private static final Connection connection = MySQLConnectionManager.getInstance().getConnection();

    private long timeout = 0;

    private final long QUERY_EXECUTION_TIMEOUT = 30000; //30 seconds

    public MySQLQueryManager(long timeout){
        this.timeout = timeout + QUERY_EXECUTION_TIMEOUT;
    }

    public MySQLQueryManager(){
        this.timeout = QUERY_EXECUTION_TIMEOUT;
    }

    public void increaseTimeout(long timeout) {
        this.timeout +=  timeout;
    }

    public QueryResult runWithThread(String query, QueryResult queryResult) {

        Statement statement = null;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<QueryResult> future = null;
        try {
            statement = connection.createStatement();
            QueryExecutor queryExecutor = new QueryExecutor(statement, query, queryResult);
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


    private class QueryExecutor implements  Callable<QueryResult>{

        Statement statement;
        String query;
        QueryResult queryResult;

        public QueryExecutor(Statement statement, String query, QueryResult queryResult) {
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

    public float checkSelectivity(String queryPredicates) {
        QueryResult queryResult = runTimedQueryWithOutSorting(queryPredicates, true);
        return (float) queryResult.getResultCount() / (float) PolicyConstants.NUMBER_OR_TUPLES;
    }

    public float checkSelectivityFullQuery(String query) {
        QueryResult queryResult = runTimedQueryWithOutSorting(query);
        return (float) queryResult.getResultCount() / (float) PolicyConstants.NUMBER_OR_TUPLES;
    }

    /**
     * Compute the cost by execution time of the query and writes the results to file
     * @return
     * @throws PolicyEngineException
     */

    public QueryResult runTimedQueryExp(String query, int repetitions) throws PolicyEngineException {
        try {
            QueryResult queryResult = new QueryResult();
            queryResult.setResultsCheck(false);
            List<Long> gList = new ArrayList<>();
            for (int i = 0; i < repetitions; i++) {
                gList.add(runWithThread(query, queryResult).getTimeTaken().toMillis());
            }
            Duration gCost;
            if(repetitions >= 3) {
                Collections.sort(gList);
                List<Long> clippedGList = gList.subList(1, repetitions - 1);
                gCost = Duration.ofMillis(clippedGList.stream().mapToLong(i -> i).sum() / clippedGList.size());
            }
            else{
                gCost =  Duration.ofMillis(gList.stream().mapToLong(i -> i).sum() / gList.size());
            }
            queryResult.setTimeTaken(gCost);
            return queryResult;
        } catch (Exception e) {
            throw new PolicyEngineException("Error Running Query");
        }
    }

    /**
     * Compute the cost by execution time of the query and writes the results to file
     * @param predicates
     * @param resultCheck
     * @return
     * @throws PolicyEngineException
     */

    public QueryResult runTimedQueryWithRepetitions(String predicates, Boolean resultCheck, int repetitions) throws PolicyEngineException {
        try {
            QueryResult queryResult = new QueryResult();
            queryResult.setResultsCheck(resultCheck);
            List<Long> gList = new ArrayList<>();
            for (int i = 0; i < repetitions; i++)
                gList.add(runWithThread(PolicyConstants.SELECT_ALL_SEMANTIC_OBSERVATIONS_WHERE + predicates,
                        queryResult).getTimeTaken().toMillis());
            Duration gCost;
            if(repetitions >= 3) {
                Collections.sort(gList);
                List<Long> clippedGList = gList.subList(1, repetitions - 1);
                gCost = Duration.ofMillis(clippedGList.stream().mapToLong(i -> i).sum() / clippedGList.size());
            }
            else{
                gCost =  Duration.ofMillis(gList.stream().mapToLong(i -> i).sum() / gList.size());

            }
            queryResult.setTimeTaken(gCost);
            return queryResult;
        } catch (Exception e) {
            throw new PolicyEngineException("Error Running Query");
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
            QueryResult queryResult = new QueryResult();
            return runWithThread(PolicyConstants.SELECT_ALL_SEMANTIC_OBSERVATIONS + predicates, queryResult).getTimeTaken();
        } catch (Exception e) {
            throw new PolicyEngineException("Error Running Query");
        }
    }

    /**
     * Compute the cost by execution time of the query which includes a subquery clause
     * @param completeQuery - including the FROM clause
     * @return
     * @throws PolicyEngineException
     */

    public QueryResult runTimedSubQuery(String completeQuery, boolean resultCheck) throws PolicyEngineException {
        try {
            QueryResult queryResult = new QueryResult();
            queryResult.setResultsCheck(resultCheck);
            return runWithThread(completeQuery, queryResult);
        } catch (Exception e) {
            throw new PolicyEngineException("Error Running Query");
        }
    }


    /**
     * Execution time for guards which includes cost of sorting the results
     * @param predicates
     * @return
     * @throws PolicyEngineException
     */
    public QueryResult runTimedQueryWithOutSorting(String predicates, boolean where) throws PolicyEngineException {
        try {
            QueryResult queryResult = new QueryResult();
            if(!where)
                return runWithThread(PolicyConstants.SELECT_ALL_SEMANTIC_OBSERVATIONS + predicates, queryResult);
            else
                return runWithThread(PolicyConstants.SELECT_ALL_SEMANTIC_OBSERVATIONS_WHERE + predicates, queryResult);
        } catch (Exception e) {
            throw new PolicyEngineException("Error Running Query");
        }
    }

    /**
     * @param full_query
     * @return
     * @throws PolicyEngineException
     */
    public QueryResult runTimedQueryWithOutSorting(String full_query) throws PolicyEngineException {
        try {
            QueryResult queryResult = new QueryResult();
            return runWithThread(full_query, queryResult);
        } catch (Exception e) {
            throw new PolicyEngineException("Error Running Query");
        }
    }


    /**
     * Execution time for guards which doesn't cost of sorting the results
     * @throws PolicyEngineException
     */
    public QueryResult executeQuery(String predicates, boolean where, int repetitions) throws PolicyEngineException {
        try {
            QueryResult queryResult = new QueryResult();
            String query;
            if (!where)
                query = PolicyConstants.SELECT_ALL_SEMANTIC_OBSERVATIONS + predicates;
            else
                query = PolicyConstants.SELECT_ALL_SEMANTIC_OBSERVATIONS_WHERE + predicates;
            List<Long> gList = new ArrayList<>();
            for (int i = 0; i < repetitions; i++)
                gList.add(runWithThread(query, queryResult).getTimeTaken().toMillis());
            Duration gCost;
            if (repetitions >= 3) {
                Collections.sort(gList);
                List<Long> clippedGList = gList.subList(1, repetitions - 1);
                gCost = Duration.ofMillis(clippedGList.stream().mapToLong(i -> i).sum() / clippedGList.size());
            } else {
                gCost = Duration.ofMillis(gList.stream().mapToLong(i -> i).sum() / gList.size());

            }
            queryResult.setTimeTaken(gCost);
            return queryResult;
        } catch (Exception e) {
            throw new PolicyEngineException("Error Running Query");
        }

    }
}