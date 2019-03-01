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

    private static long timeout = 300000;

    public MySQLResult runWithThread(String query, MySQLResult mySQLResult) {

        Statement statement = null;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<MySQLResult> future = null;
        try {
            statement = connection.createStatement();
            QueryExecutor queryExecutor = new QueryExecutor(statement, query, mySQLResult);
            future = executor.submit(queryExecutor);
            mySQLResult = future.get(timeout, TimeUnit.MILLISECONDS);
            executor.shutdown();
            return mySQLResult;
        } catch (SQLException | InterruptedException | ExecutionException ex) {
            cancelStatement(statement, ex);
            ex.printStackTrace();
            throw new PolicyEngineException("Failed to query the database. " + ex);
        } catch (TimeoutException ex) {
            cancelStatement(statement, ex);
            future.cancel(true);
            mySQLResult.setTimeTaken(PolicyConstants.MAX_DURATION);
            return mySQLResult;
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


    private class QueryExecutor implements  Callable<MySQLResult>{

        Statement statement;
        String query;
        MySQLResult mySQLResult;

        public QueryExecutor(Statement statement, String query, MySQLResult mySQLResult) {
            this.statement = statement;
            this.query = query;
            this.mySQLResult = mySQLResult;
        }

        @Override
        public MySQLResult call() throws Exception {
            try {
                Instant start = Instant.now();
                ResultSet rs = statement.executeQuery(query);
                Instant end = Instant.now();
                if(mySQLResult.getResultsCheck())
                    mySQLResult.setQueryResult(rs);
                int rowcount = 0;
                if (hasColumn(rs, "total")){
                    rs.next();
                    rowcount = rs.getInt(1);
                }
                else if (rs.last()) {
                    rowcount = rs.getRow();
                }
                if(mySQLResult.getPathName() != null && mySQLResult.getFileName() != null){
                    mySQLResult.writeResultsToFile(rs);
                }
                mySQLResult.setResultCount(rowcount);
                mySQLResult.setTimeTaken(Duration.between(start, end));
                return mySQLResult;
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


    /**
     * Compute the cost by execution time of the query and writes the results to file
     * @param predicates
     * @param resultCheck
     * @return
     * @throws PolicyEngineException
     */

    public MySQLResult runTimedQuery(String predicates, Boolean resultCheck, int repetitions) throws PolicyEngineException {
        try {
            MySQLResult mySQLResult = new MySQLResult();
            mySQLResult.setResultsCheck(resultCheck);
            List<Long> gList = new ArrayList<>();
            for (int i = 0; i < repetitions; i++)
                gList.add(runWithThread(PolicyConstants.SELECT_ALL_SEMANTIC_OBSERVATIONS + predicates,
                        mySQLResult).getTimeTaken().toMillis());
            Duration gCost;
            if(repetitions >= 3) {
                Collections.sort(gList);
                List<Long> clippedGList = gList.subList(1, repetitions - 1);
                gCost = Duration.ofMillis(clippedGList.stream().mapToLong(i -> i).sum() / clippedGList.size());
            }
            else{
                gCost =  Duration.ofMillis(gList.stream().mapToLong(i -> i).sum() / gList.size());

            }
            mySQLResult.setTimeTaken(gCost);
            return mySQLResult;
        } catch (Exception e) {
            e.printStackTrace();
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
            MySQLResult mySQLResult = new MySQLResult();
            return runWithThread(PolicyConstants.SELECT_ALL_SEMANTIC_OBSERVATIONS + predicates, mySQLResult).getTimeTaken();
        } catch (Exception e) {
            e.printStackTrace();
            throw new PolicyEngineException("Error Running Query");
        }
    }

    public MySQLResult runTimedQueryWithResultCount(String predicates) throws PolicyEngineException {
        try {
            MySQLResult mySQLResult = new MySQLResult();
            return runWithThread(PolicyConstants.SELECT_COUNT_STAR_SEMANTIC_OBSERVATIONS + predicates, mySQLResult);
        } catch (Exception e) {
            e.printStackTrace();
            throw new PolicyEngineException("Error Running Query");
        }
    }


// explaining query
//    public Duration explainQuery(PreparedStatement stmt, int queryNum){
//        try {
//            Instant start = Instant.now();
//            ResultSet rs = stmt.executeQuery();
//            ResultSetMetaData rsmd = rs.getMetaData();
//            int columnsNumber = rsmd.getColumnCount();
//
//            RowWriter<String> writer = new RowWriter<>(outputDir+"/explains/", getDatabase(), mapping, getFileFromQuery(queryNum));
//            while(rs.next()) {
//                StringBuilder line = new StringBuilder("");
//                for(int i = 1; i <= columnsNumber; i++)
//                    line.append(rs.getString(i)).append("\t");
//                writer.writeString(line.toString());
//            }
//            writer.close();
//            rs.close();
//            Instant end = Instant.now();
//            return Duration.between(start, end);
//        } catch (SQLException | IOException e) {
//            e.printStackTrace();
//        }
//    }

}