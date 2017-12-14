package edu.uci.ics.tippers.data;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.model.query.RangeQuery;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.Date;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by cygnus on 12/12/17.
 * Heavily borrowed from Benchmark code
 */
public class PolicyExecution {

    private long timeout = 50000;

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private Connection connection;

    private static final int[] policyNumbers = {10, 100, 1000, 5000, 10000};

    private PolicyGeneration policyGen;

    public PolicyExecution(){
        this.connection = MySQLConnectionManager.getInstance().getConnection();
        policyGen = new PolicyGeneration();
    }


    public Duration runTimedQuery(String query) {
        try {
            Instant start = Instant.now();
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            while (rs.next()) {
                StringBuilder line = new StringBuilder("");
                for (int i = 1; i <= columnsNumber; i++)
                    line.append(rs.getString(i)).append("\t");
            }
            rs.close();
            Instant end = Instant.now();
            return Duration.between(start, end);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new PolicyEngineException("Error Running Query");
        }
    }

    private Duration runWithThread(Callable<Duration> query) {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<Duration> future = executorService.submit(query);
        try {
            return future.get(timeout, TimeUnit.MILLISECONDS);
        }catch (TimeoutException e) {
            e.printStackTrace();
            return PolicyConstants.MAX_DURATION;
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            throw new PolicyEngineException("Error Running Query");
        } finally {
            executorService.shutdown();
            try {
                executorService.awaitTermination(PolicyConstants.SHUTDOWN_WAIT, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public Map<String, Duration> runBasicQueries(String policyDir) {

        Map<String, Duration> policyRunTimes = new HashMap<>();

        File[] policyFiles = new File(policyDir).listFiles();

        for (File file : policyFiles) {

            String values = null;
            try {
                values = new String(Files.readAllBytes(Paths.get(policyDir + file.getName())),
                        StandardCharsets.UTF_8);
            } catch (IOException e) {
                e.printStackTrace();
                throw new PolicyEngineException("Error Reading Data Files");
            }
            List<String> users = new ArrayList<>();
            List<String> locations = new ArrayList<>();
            List<Date> timeStamps = new ArrayList<>();
            List<String> temperatures = new ArrayList<>();
            List<String> wemos = new ArrayList<>();
            List<String> activities = new ArrayList<>();

            JSONArray jsonArray = new JSONArray(values);
            jsonArray.forEach(e->{
                users.add(((JSONObject)e).getString("user_id"));
                locations.add(((JSONObject)e).getString("location_id"));
                try {
                    timeStamps.add(sdf.parse(((JSONObject)e).getString("timestamp")));
                } catch (ParseException e1) {
                    e1.printStackTrace();
                }
                temperatures.add(((JSONObject)e).getString("temperature"));
                wemos.add(((JSONObject)e).getString("wemo"));
                activities.add(((JSONObject)e).getString("activity"));
            });

            int numQueries = 0;
            Duration runTime = Duration.ofSeconds(0);

            try {
                runTime = runTime.plus(runWithThread(() -> runBasicQuery(users, locations, timeStamps, temperatures, wemos, activities)));
                numQueries++;
                policyRunTimes.put(file.getName(), runTime.dividedBy(numQueries));

            } catch (Exception e) {
                e.printStackTrace();
                policyRunTimes.put(file.getName(), PolicyConstants.MAX_DURATION);
            }
        }

        return policyRunTimes;

    }

    public Duration runBasicQuery(List<String> users, List<String> locations, List<Date> timeStamps,
                              List<String> temperatures, List<String> wemos, List<String> activities){

        String query = "SELECT * FROM SEMANTIC_OBSERVATION " +
                "WHERE " +
                IntStream.range(0, users.size()-1).mapToObj(i->
                        String.format("(USER_ID = '%s' AND LOCATION_ID = '%s' AND timeStamp = '%s' AND temperature = '%s' AND energy = '%s' AND activity = '%s')",
                                users.get(i), locations.get(i), sdf.format(timeStamps.get(i)), temperatures.get(i), wemos.get(i), activities.get(i)))
                        .collect(Collectors.joining(" OR "))
                + ";";
        try {
            return runTimedQuery(query);
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

            int numQueries = 0;
            Duration runTime = Duration.ofSeconds(0);

            try {
                runTime = runTime.plus(runWithThread(() -> runRangeQuery(rangeQueries)));
                numQueries++;
                policyRunTimes.put(file.getName(), runTime.dividedBy(numQueries));

            } catch (Exception e) {
                e.printStackTrace();
                policyRunTimes.put(file.getName(), PolicyConstants.MAX_DURATION);
            }
        }

        return policyRunTimes;
    }


    public Duration runRangeQuery(List<RangeQuery> rangeQueries){
        String query = "SELECT * FROM SEMANTIC_OBSERVATION " +
                "WHERE " + IntStream.range(0, rangeQueries.size()-1 ).mapToObj(i-> rangeQueries.get(i).createPredicate())
                .collect(Collectors.joining(" OR "));
        try {
            return runTimedQuery(query);
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

    private void basicQueryExperiments(){
        for (int i = 0; i < policyNumbers.length ; i++) {
            policyGen.generateBasicPolicy(policyNumbers[i], 6);
        }
        Map<String, Duration> runTimes = new HashMap<>();
        runTimes.putAll(runBasicQueries(PolicyConstants.BASIC_POLICY_DIR));
        createTextReport(runTimes, PolicyConstants.BASIC_POLICY_DIR);

    }


    private void rangeQueryExperiments(){
        for (int i = 0; i < policyNumbers.length ; i++) {
            policyGen.generateRangePolicy(policyNumbers[i]);
        }

        Map<String, Duration> runTimes = new HashMap<>();
        runTimes.putAll(runRangeQueries(PolicyConstants.RANGE_POLICY_DIR));
        createTextReport(runTimes, PolicyConstants.RANGE_POLICY_DIR);
    }



    public static void main (String args[]){
        PolicyExecution pe = new PolicyExecution();
        pe.basicQueryExperiments();
//        pe.rangeQueryExperiments();

    }
}
