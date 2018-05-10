package edu.uci.ics.tippers.db;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import edu.uci.ics.tippers.fileop.Reader;
import edu.uci.ics.tippers.fileop.Writer;
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

    Writer writer = new Writer();

    public Duration runWithThread(String query, String result_file) {

        Statement statement = null;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Duration> future = null;
        try {
            statement = connection.createStatement();
            QueryExecutor queryExecutor = new QueryExecutor(statement, query, result_file);
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
        String result_file;

        public QueryExecutor(Statement statement, String query, String result_file) {
            this.statement = statement;
            this.query = query;
            this.result_file = result_file;
        }

        @Override
        public Duration call() throws Exception {
            List<Semantic_Observation> query_results = new ArrayList<>();
            try {
                Instant start = Instant.now();
                ResultSet rs = statement.executeQuery(query);
                Instant end = Instant.now();
                while(rs.next()){
                    Semantic_Observation so = new Semantic_Observation();
                    so.setId(rs.getString("id"));
                    so.setUser_id(rs.getString("user_id"));
                    so.setLocation(rs.getString("location_id"));
                    so.setTimeStamp(rs.getString("timeStamp"));
                    so.setTemperature(rs.getString("temperature"));
                    so.setEnergy(rs.getString("energy"));
                    so.setActivity(rs.getString("activity"));
                    query_results.add(so);
                }
                System.out.println("Size of Result set: " + query_results.size());
                rs.close();
                if(result_file != null)
                    writer.writeJSONToFile(query_results, PolicyConstants.QUERY_RESULTS_DIR, result_file);
                return Duration.between(start, end);
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

    public Duration runTimedQuery(String predicates, String result_file) throws PolicyEngineException {
        try {
            return runWithThread(PolicyConstants.SELECT_ALL_SEMANTIC_OBSERVATIONS + predicates, result_file);
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
        Comparator<Semantic_Observation> comp =
                Comparator.comparingInt(so -> Integer.parseInt(so.getId()));
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

    //TODO: Temporary counting query ** clean up **
    public long runCountingQuery(String query){
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

}
