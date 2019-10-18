package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.db.MySQLResult;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.manager.PolicyPersistor;
import edu.uci.ics.tippers.model.guard.GuardHit;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.query.QueryStatement;
import edu.uci.ics.tippers.generation.query.QueryGeneration;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class Experiment2 {

    private Writer writer;
    private MySQLQueryManager mySQLQueryManager;
    QueryGeneration queryGeneration;
    PolicyPersistor polper;


    public Experiment2() {
        writer = new Writer();
        mySQLQueryManager = new MySQLQueryManager();
        queryGeneration = new QueryGeneration();
        polper = new PolicyPersistor();
    }

    private void runBEPolicies() {

        List<BEPolicy> policies = polper.retrievePolicies("10", "user");
        BEExpression beExpression = new BEExpression(policies);
        System.out.println("Original Policies: " + beExpression.createQueryFromPolices());

        List<QueryStatement> highQueries = queryGeneration.retrieveQueries("high", 10);
        List<QueryStatement> mediumQueries = queryGeneration.retrieveQueries("medium", 10);
        List<QueryStatement> lowQueries = queryGeneration.retrieveQueries("low", 10);
        Map<String, List<QueryStatement>> queries = new HashMap<>();
        queries.put("high", highQueries);
        queries.put("low", lowQueries);
        queries.put("medium", mediumQueries);

        boolean EXTEND_PREDICATES = true;
        Duration guardGen = Duration.ofMillis(0);
        Instant fsStart = Instant.now();
        GuardHit gh = new GuardHit(beExpression, EXTEND_PREDICATES);
        Instant fsEnd = Instant.now();
        guardGen = guardGen.plus(Duration.between(fsStart, fsEnd));
        System.out.println("Guard Generation time: " + guardGen + " Number of Guards: " + gh.numberOfGuards());

        TreeMap<String, String> policyRunTimes = new TreeMap<>();
        int numOfRepetitions = 3;

        String exptResultsFile = "results.csv";
        String policyDir = PolicyConstants.BE_POLICY_DIR;


        int queryCount = 1;
        for (String key : queries.keySet()) {
            policyRunTimes.clear();
            policyRunTimes.put(key, "12345");
            writer.appendToCSVReport(policyRunTimes, policyDir, exptResultsFile);
            TreeMap<String, String> queryRunTimes = new TreeMap<>(Comparator.comparingInt(Integer::parseInt));
            TreeMap<String, String> queryPolicyRunTimes = new TreeMap<>(Comparator.comparingInt(Integer::parseInt));
            List<String> query_ids = queries.get(key).stream().map(qs -> String.valueOf(qs.getId())).collect(Collectors.toList());
            writer.writeToFile(query_ids, key + "_queries.txt", PolicyConstants.BE_POLICY_DIR);
            for (QueryStatement qs : queries.get(key)) {
                try {
                    /** Traditional approach **/
                    String query = qs.getQuery();
                    Duration runTime = Duration.ofMillis(0);
                    String tradQuery = "(" + query + ") AND (" + beExpression.createQueryFromPolices() + ")";
                    MySQLResult tradResult = mySQLQueryManager.runTimedQueryWithRepetitions(tradQuery, false, numOfRepetitions);
                    runTime = runTime.plus(tradResult.getTimeTaken());
                    queryPolicyRunTimes.put(String.valueOf(queryCount), String.valueOf(runTime.toMillis()));
//                    System.out.println("** " + file.getName() + " " + queryCount + " completed and took " + runTime.toMillis());
                    MySQLResult queryResult = mySQLQueryManager.runTimedQueryWithRepetitions(query, false, numOfRepetitions);
                    queryRunTimes.put(String.valueOf(queryCount), String.valueOf(queryResult.getTimeTaken().toMillis()));
                    queryCount += 1;
                } catch (Exception e) {
                    e.printStackTrace();
//                    queryPolicyRunTimes.put(file.getName(), PolicyConstants.MAX_DURATION.toString());
//                    queryRunTimes.put(file.getName(), PolicyConstants.MAX_DURATION.toString());
                }
            }
//            writer.appendToCSVReport(queryRunTimes, policyDir, exptResultsFile);
//            writer.appendToCSVReport(queryPolicyRunTimes, policyDir, exptResultsFile);
        }
    }



public static void main(String args[]){
        Experiment2 ex2 = new Experiment2();
        ex2.runBEPolicies();
    }
}
