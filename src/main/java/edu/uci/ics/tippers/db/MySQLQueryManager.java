package edu.uci.ics.tippers.db;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import edu.uci.ics.tippers.fileop.Reader;
import edu.uci.ics.tippers.model.data.Semantic_Observation;
import org.apache.commons.dbutils.DbUtils;

import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.IntStream;

/**
 * Created by cygnus on 10/29/17.
 */
public class MySQLQueryManager {

    private static final Connection connection = MySQLConnectionManager.getInstance().getConnection();

    private static long timeout = 300000;

    public MySQLResult runWithThread(String query) {

        Statement statement = null;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<MySQLResult> future = null;
        try {
            statement = connection.createStatement();
            QueryExecutor queryExecutor = new QueryExecutor(statement, query);
            future = executor.submit(queryExecutor);
            MySQLResult mySQLResult = future.get(timeout, TimeUnit.MILLISECONDS);
            executor.shutdown();
            return mySQLResult;
        } catch (SQLException | InterruptedException | ExecutionException ex) {
            cancelStatement(statement, ex);
            throw new PolicyEngineException("Failed to query the database. " + ex);
        } catch (TimeoutException ex) {
            cancelStatement(statement, ex);
            future.cancel(true);
            return new MySQLResult(PolicyConstants.MAX_DURATION);
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

        public QueryExecutor(Statement statement, String query) {
            this.statement = statement;
            this.query = query;
        }

        @Override
        public MySQLResult call() throws Exception {
            List<Semantic_Observation> query_results = new ArrayList<>();
            try {
                Instant start = Instant.now();
                ResultSet rs = statement.executeQuery(query);
                Instant end = Instant.now();
                return new MySQLResult(rs, Duration.between(start, end));
            } catch (SQLException e) {
                System.out.println("Exception raised by : " + query);
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
            return runWithThread(PolicyConstants.SELECT_ALL_SEMANTIC_OBSERVATIONS + predicates).getDuration();
        } catch (Exception e) {
            e.printStackTrace();
            throw new PolicyEngineException("Error Running Query");
        }
    }

    public MySQLResult executeGuard(String predicates, String table_name) throws PolicyEngineException {
        String query = String.format("SELECT * FROM %s WHERE "+ predicates, table_name);
        try {
            return runWithThread(query);
        } catch (Exception e) {
            e.printStackTrace();
            throw new PolicyEngineException("Error Running Query");
        }
    }

    /**
     * Check the results against the traditional query rewritten approach
     * @param filename
     * @return
     */
    public Boolean checkResults(String filename) {
        String ogFile = filename.substring(filename.indexOf("/")+1);
        List<Semantic_Observation> og = parseJSONList(Reader.readTxt(PolicyConstants.QUERY_RESULTS_DIR + ogFile +".json"));
        List<Semantic_Observation> tbc = parseJSONList(Reader.readTxt(PolicyConstants.QUERY_RESULTS_DIR + filename + ".json"));
        if(og.size() != tbc.size()) return false;
        Comparator<Semantic_Observation> comp = Comparator.comparingInt(so -> Integer.parseInt(so.getId()));
        og.sort(comp);
        tbc.sort(comp);
        return IntStream.range(0, og.size())
                .allMatch(i -> comp.compare(og.get(i), tbc.get(i)) == 0);
    }

    public List<Semantic_Observation> parseJSONList(String jsonData) {
        ObjectMapper objectMapper = new ObjectMapper();
        List<Semantic_Observation> query_results = null;
        try {
            query_results = objectMapper.readValue(jsonData, new TypeReference<List<Semantic_Observation>>(){});
        } catch (IOException e) {
            e.printStackTrace();
        }
        return query_results;
    }

}
