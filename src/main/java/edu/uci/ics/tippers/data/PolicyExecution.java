package edu.uci.ics.tippers.data;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.model.query.BasicQuery;
import edu.uci.ics.tippers.model.query.RangeQuery;
import org.apache.commons.dbutils.DbUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by cygnus on 12/12/17.
 * Heavily borrowed from Benchmark code
 */
public class PolicyExecution {

    private long timeout = 25000;

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private Connection connection;

    private static final int[] policyNumbers = {5, 10, 100, 500, 1000, 2000, 5000};

    private static PolicyGeneration policyGen;

    Writer mWriter;

    public PolicyExecution(){
        this.connection = MySQLConnectionManager.getInstance().getConnection();
        policyGen = new PolicyGeneration();
        mWriter = new Writer();
    }

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

    private void cancelStatement(Statement statement, Exception ex) {
        System.out.println("Cancelling the current query statement. Timeout occurred" + ex);
        try {
            statement.cancel();
        } catch (SQLException exception) {
            throw new PolicyEngineException("Calling cancel() on the Statement issued exception. Details are: " + exception);
        }
    }


    private List<BasicQuery> readBasicPolicy(String fileName){
        String values;
        try {
            values = new String(Files.readAllBytes(Paths.get(fileName)),
                    StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
            throw new PolicyEngineException("Error Reading Data Files");
        }

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setDateFormat(sdf);
        List<BasicQuery> basicQueries = new ArrayList<BasicQuery>();
        try {
            basicQueries.addAll(objectMapper.readValue(values,
                    new TypeReference<List<BasicQuery>>() {
                    }));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return basicQueries;
    }

    private List<RangeQuery> readRangePolicy(String fileName){
        String values;
        try {
            values = new String(Files.readAllBytes(Paths.get(fileName)),
                    StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
            throw new PolicyEngineException("Error Reading Data Files");
        }

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setDateFormat(sdf);
        List<RangeQuery> rangeQueries = new ArrayList<RangeQuery>();
        try {
            rangeQueries.addAll(objectMapper.readValue(values,
                    new TypeReference<List<RangeQuery>>() {
                    }));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return rangeQueries;
    }

    public Duration runBasicQuery(List<BasicQuery> basicQueries){
        String query = "SELECT * FROM SEMANTIC_OBSERVATION " +
                "WHERE " + IntStream.range(0, basicQueries.size()-1 ).mapToObj(i-> basicQueries.get(i).createPredicate())
                .collect(Collectors.joining(" OR "));
        try {
            return runWithThread(query);
        } catch (Exception e) {
            e.printStackTrace();
            throw new PolicyEngineException("Error Running Query");
        }
    }

    public Map<String, Duration> runBasicQueries(String policyDir) {

        Map<String, Duration> policyRunTimes = new HashMap<>();

        File[] policyFiles = new File(policyDir).listFiles();

        for (File file : policyFiles) {

            List<BasicQuery> basicQueries = readBasicPolicy(policyDir + file.getName());

            Duration runTime = Duration.ofSeconds(0);

            try {
                runTime = runTime.plus(runBasicQuery(basicQueries));
                policyRunTimes.put(file.getName(), runTime);
            } catch (Exception e) {
                e.printStackTrace();
                policyRunTimes.put(file.getName(), PolicyConstants.MAX_DURATION);
            }
        }
        return policyRunTimes;
    }

    public Map<String, Duration> runRangeQueries(String policyDir) {

        Map<String, Duration> policyRunTimes = new HashMap<>();

        File[] policyFiles = new File(policyDir).listFiles();

        String values = null;

        for (File file : policyFiles) {

            try {
                values = new String(Files.readAllBytes(Paths.get(policyDir + file.getName())),
                        StandardCharsets.UTF_8);
            } catch (IOException e) {
                e.printStackTrace();
                throw new PolicyEngineException("Error Reading Data Files");
            }

            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.setDateFormat(sdf);
            List<RangeQuery> rangeQueries = new ArrayList<RangeQuery>();
            try {
                rangeQueries.addAll(objectMapper.readValue(values,
                        new TypeReference<List<RangeQuery>>() {
                        }));
            } catch (IOException e) {
                e.printStackTrace();
            }

            Duration runTime = Duration.ofSeconds(0);

            try {
                runTime = runTime.plus(runRangeQuery(rangeQueries));
                policyRunTimes.put(file.getName(), runTime);
            } catch (Exception e) {
                e.printStackTrace();
                policyRunTimes.put(file.getName(), PolicyConstants.MAX_DURATION);
            }
        }

        return policyRunTimes;
    }

    public Duration runRangeQuery(List<RangeQuery> rangeQueries){
        String query = "SELECT * FROM SEMANTIC_OBSERVATION " +
                "WHERE " + IntStream.range(0, rangeQueries.size() ).mapToObj(i-> rangeQueries.get(i).createPredicate())
                .collect(Collectors.joining(" OR "));

        try {
            return runWithThread(query);
        } catch (Exception e) {
            e.printStackTrace();
            throw new PolicyEngineException("Error Running Query");
        }
    }



    private void createTextReport(Map<String, Duration> runTimes, String fileDir) {
        BufferedWriter writer;
        try {
            writer = new BufferedWriter(new FileWriter( fileDir + "results.txt"));

            writer.write("Number of Policies     Time taken (in ms)");
            writer.write("\n\n");

            String line = "";
            for (String policy: runTimes.keySet()) {
                if (runTimes.get(policy).compareTo(PolicyConstants.MAX_DURATION) < 0 )
                    line +=  String.format("%s %s", policy, runTimes.get(policy).toMillis());
                else
                    line +=  String.format("%s  %s", policy, "Timed out" );
                line += "\n";
            }
            writer.write(line);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void generatePolicies(String policyDir){
        for (int i = 0; i < policyNumbers.length ; i++) {
            if(policyDir.equalsIgnoreCase(PolicyConstants.BASIC_POLICY_1_DIR))
                policyGen.generateBasicPolicy1(policyNumbers[i]);
            else if(policyDir.equalsIgnoreCase(PolicyConstants.BASIC_POLICY_2_DIR))
                policyGen.generateBasicPolicy2(policyNumbers[i]);
            else if(policyDir.equalsIgnoreCase(PolicyConstants.RANGE_POLICY_1_DIR))
                policyGen.generateRangePolicy1(policyNumbers[i]);
            else if(policyDir.equalsIgnoreCase(PolicyConstants.RANGE_POLICY_2_DIR))
                policyGen.generateRangePolicy2(policyNumbers[i]);
        }
    }

    private void basicQueryExperiments(String policyDir){
        Map<String, Duration> runTimes = new HashMap<>();
        runTimes.putAll(runBasicQueries(policyDir));
        createTextReport(runTimes, policyDir);
    }


    private void rangeQueryExperiments(String policyDir){
        Map<String, Duration> runTimes = new HashMap<>();
        runTimes.putAll(runRangeQueries(policyDir));
        createTextReport(runTimes, policyDir);
    }



    public static void main (String args[]){
        PolicyExecution pe = new PolicyExecution();
//        pe.generatePolicies(PolicyConstants.BASIC_POLICY_1_DIR);
//        pe.basicQueryExperiments(PolicyConstants.BASIC_POLICY_1_DIR);
//        pe.generatePolicies(PolicyConstants.BASIC_POLICY_2_DIR);
//        pe.basicQueryExperiments(PolicyConstants.BASIC_POLICY_2_DIR);
//        pe.generatePolicies(PolicyConstants.RANGE_POLICY_1_DIR);
//        pe.rangeQueryExperiments(PolicyConstants.RANGE_POLICY_1_DIR);
        pe.generatePolicies(PolicyConstants.RANGE_POLICY_2_DIR);
//        pe.rangeQueryExperiments(PolicyConstants.RANGE_POLICY_2_DIR);

    }
}
