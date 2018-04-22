package edu.uci.ics.tippers.data;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.fileop.Reader;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.model.guard.ApproxFactorization;
import edu.uci.ics.tippers.model.guard.GreedyExact;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;
import edu.uci.ics.tippers.model.query.BasicQuery;
import edu.uci.ics.tippers.model.query.RangeQuery;
import org.apache.commons.dbutils.DbUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.text.DateFormat;
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

    private long timeout = 250000;

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private Connection connection;

    private static final int[] policyNumbers = {2000, 3000, 4000, 10000, 50000, 100000};

    private static PolicyGeneration policyGen;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private BufferedWriter writer;


    Writer mWriter;

    public PolicyExecution(){
        this.connection = MySQLConnectionManager.getInstance().getConnection();
        policyGen = new PolicyGeneration();
        mWriter = new Writer();
        objectMapper.setDateFormat(sdf);
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
                int size = 0;
                if (rs != null)
                {
                    rs.beforeFirst();
                    rs.last();
                    size = rs.getRow();
                }
                rs.close();
                Instant end = Instant.now();
                System.out.println("Result set size " + size);
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
        String values = Reader.readTxt(fileName);
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

    private List<RangeQuery> readRangePolicy(String fileName){
        String values = Reader.readTxt(fileName);
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

    public Map<String, Duration> runRangeQueries(String policyDir) {

        Map<String, Duration> policyRunTimes = new HashMap<>();

        File[] policyFiles = new File(policyDir).listFiles();

        String values = null;

        for (File file : policyFiles) {

            List<RangeQuery> rangeQueries = readRangePolicy(policyDir + file.getName());

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


    public Duration runQuery(String query){
        try {
            return runWithThread(PolicyConstants.SELECT_ALL_SEMANTIC_OBSERVATIONS  + query);
        } catch (Exception e) {
            e.printStackTrace();
            throw new PolicyEngineException("Error Running Query");
        }
    }

    public Map<String, Duration> runBEPolicies(String policyDir) {

        Map<String, Duration> policyRunTimes = new HashMap<>();

        File[] policyFiles = new File(policyDir).listFiles();

        for (File file : policyFiles) {

            System.out.println(file.getName() + " being processed........");

            BEExpression beExpression = new BEExpression();

            beExpression.parseJSONList(Reader.readTxt(policyDir + file.getName()));

            int pred_count = beExpression.getPolicies().stream()
                    .map(BEPolicy::getObject_conditions)
                    .filter(objectConditions ->  objectConditions != null)
                    .mapToInt(List::size)
                    .sum();

            System.out.println("Original number of Predicates :" + pred_count);

            Duration runTime = Duration.ofMillis(0);

            try {
                runTime = runTime.plus(runQuery(beExpression.createQueryFromPolices()));
                policyRunTimes.put(file.getName(), runTime);
                System.out.println(file.getName() + " completed and took " + runTime);


                runTime = Duration.ofSeconds(0);
                ApproxFactorization f = new ApproxFactorization(beExpression);
                f.approximateFactorization();
                System.out.print("Approximate number of tuples: ");
                runQuery(f.getExpression().createQueryFromPolices());
                runTime = runTime.plus(runQuery(f.getExpression().createQueryFromPolices()));
                policyRunTimes.put(file.getName() + "-af", runTime);
                System.out.println("Approx Factorization complete amd took " + runTime);
                writeJSONToFile(f.getExpression().getPolicies(), file.getName() + "-af");

                /** To read approximate expression from the file **/
//                BEExpression approxExpression = new BEExpression();
//                approxExpression.parseJSONList(Reader.readTxt(policyDir + file.getName()));

                GreedyExact gf = new GreedyExact(f.getExpression());
                gf.GFactorize();
                System.out.println("Greedy Factorization complete ");
                runTime = Duration.ofMillis(0);
                runTime = runTime.plus(runQuery(gf.createQueryFromExactFactor()));
                policyRunTimes.put(file.getName() + "-gf", runTime);
                System.out.println(file.getName() + "_greedy_factorized completed and took " + runTime);

            } catch (Exception e) {
                e.printStackTrace();
                policyRunTimes.put(file.getName(), PolicyConstants.MAX_DURATION);
            }
        }

        return policyRunTimes;
    }

    //TODO: Clean up and have a common method for writing to files
    //For writing policies to BE_POLICY_DIR
    private void writeJSONToFile(List<?> policies, String fileName){
        ObjectMapper mapper = new ObjectMapper();
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        mapper.setDateFormat(formatter);
        ObjectWriter writer = mapper.writer(new DefaultPrettyPrinter());
        try {
            writer.writeValue(new File(PolicyConstants.BE_POLICY_DIR + fileName), policies);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void createTextReport(Map<String, Duration> runTimes, String fileDir) {
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
            else if(policyDir.equalsIgnoreCase(PolicyConstants.BE_POLICY_DIR))
                policyGen.generateBEPolicy(policyNumbers[i]);
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

    private void bePolicyExperiments(String policyDir){
        Map<String, Duration> runTimes = new HashMap<>();
        runTimes.putAll(runBEPolicies(policyDir));
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
//        pe.generatePolicies(PolicyConstants.RANGE_POLICY_2_DIR);
//        pe.rangeQueryExperiments(PolicyConstants.RANGE_POLICY_2_DIR);

//        pe.generatePolicies(PolicyConstants.BE_POLICY_DIR);
        pe.bePolicyExperiments(PolicyConstants.BE_POLICY_DIR);
    }
}
