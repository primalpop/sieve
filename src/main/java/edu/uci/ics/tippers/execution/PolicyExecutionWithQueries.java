package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.db.MySQLResult;
import edu.uci.ics.tippers.fileop.Reader;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.model.guard.FactorExtension;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.query.QueryStatement;
import edu.uci.ics.tippers.setup.query.QueryGeneration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class PolicyExecutionWithQueries {

    private Writer writer;
    private MySQLQueryManager mySQLQueryManager;
    QueryGeneration queryGeneration;

    public PolicyExecutionWithQueries() {
        writer = new Writer();
        mySQLQueryManager = new MySQLQueryManager();
        queryGeneration = new QueryGeneration();
    }

    private void runBEPolicies(String policyDir, boolean[] templates, int numOfQueries) {

        //Filtering out only json files
        File dir = new File(policyDir);
        File[] policyFiles = dir.listFiles((dir1, name) -> name.toLowerCase().endsWith(".json"));
        Arrays.sort(policyFiles != null ? policyFiles : new File[0]);

        String exptResultsFile = "results.csv";
        String queriesFile = "queries.csv";

        try {
            Files.delete(Paths.get(policyDir + exptResultsFile));
        } catch (IOException ioException) { }

        List<QueryStatement> highQueries = queryGeneration.retrieveQueries("high", 10);
        List<QueryStatement> mediumQueries = queryGeneration.retrieveQueries("medium", 10);
        List<QueryStatement> lowQueries = queryGeneration.retrieveQueries("low", 10);
        Map<String, List<QueryStatement>> queries = new HashMap<>();
        queries.put("high", highQueries);
        queries.put("low", lowQueries);
        queries.put("medium", mediumQueries);

        if (policyFiles != null) {
            for (File file : policyFiles) {
                TreeMap<String, String> policyRunTimes = new TreeMap<>();
                System.out.println(file.getName() + " being processed......");
                BEExpression beExpression = new BEExpression();
                beExpression.parseJSONList(Reader.readTxt(policyDir + file.getName()));
                Duration runTime = Duration.ofMillis(0);
                boolean resultCheck = false;
                MySQLResult tradResult;
                int numOfRepetitions = 3;
//
//                MySQLResult policyResult = mySQLQueryManager.runTimedQueryWithOutSorting(beExpression.createQueryFromPolices());
//                System.out.println("Policy only: " + policyResult.getTimeTaken());

                System.out.println("Starting Generation......");
                Duration guardGen = Duration.ofMillis(0);
                FactorExtension f = new FactorExtension(beExpression);
                Instant feStart = Instant.now();
                int ext = f.doYourThing();
                policyRunTimes.put("Number of Extensions", String.valueOf(ext));
                Instant feEnd = Instant.now();
                guardGen = guardGen.plus(Duration.between(feStart, feEnd));
                System.out.println("Finished Predicate Extension: " + guardGen.toMillis()/1000 + " seconds");

//                FactorSearch fs = new FactorSearch(beExpression);
//                Instant fsStart = Instant.now();
//                fs.search();
//                Instant fsEnd = Instant.now();
//                guardGen = guardGen.plus(Duration.between(fsStart, fsEnd));
//                System.out.println("Finished Guard Generation, total time: " + guardGen.toMillis()/1000 + " seconds");
//                writer.addGuardReport(fs.printDetailedResults(numOfRepetitions), policyDir, exptResultsFile);
//                policyRunTimes.put(file.getName() + "-guardGeneration", String.valueOf(guardGen.toMillis()));
//                writer.appendToCSVReport(policyRunTimes, policyDir, exptResultsFile);

//                int queryCount = 1;
//                for (String key: queries.keySet()) {
//                    policyRunTimes.clear();
//                    policyRunTimes.put(key, "12345");
//                    writer.appendToCSVReport(policyRunTimes, policyDir, exptResultsFile);
//                    TreeMap<String,String> queryRunTimes = new TreeMap<>(Comparator.comparingInt(Integer::parseInt));
//                    TreeMap<String,String> queryPolicyRunTimes = new TreeMap<>(Comparator.comparingInt(Integer::parseInt));
//                    List<String> query_ids = queries.get(key).stream().map(qs -> String.valueOf(qs.getId())).collect(Collectors.toList());
//                    writer.writeToFile(query_ids, key + "_queries.txt", PolicyConstants.BE_POLICY_DIR);
//                    for (QueryStatement qs: queries.get(key)) {
//                        try {
//                            /** Traditional approach **/
//                            String query = qs.getQuery();
//                            runTime = Duration.ofMillis(0);
//                            String tradQuery = "(" + query + ") AND (" + beExpression.createQueryFromPolices() + ")";
//                            tradResult = mySQLQueryManager.runTimedQueryWithRepetitions(tradQuery, resultCheck, numOfRepetitions);
//                            runTime = runTime.plus(tradResult.getTimeTaken());
//                            queryPolicyRunTimes.put(String.valueOf(queryCount), String.valueOf(runTime.toMillis()));
//                            if (!(runTime.toMillis() == PolicyConstants.MAX_DURATION.toMillis())) resultCheck = true;
//                            System.out.println("** " + file.getName() + " " + queryCount + " completed and took " + runTime.toMillis());
//                            MySQLResult queryResult = mySQLQueryManager.runTimedQueryWithRepetitions(query, resultCheck, numOfRepetitions);
//                            queryRunTimes.put(String.valueOf(queryCount), String.valueOf(queryResult.getTimeTaken().toMillis()));
//                            queryCount+= 1;
//                        }catch (Exception e) {
//                            e.printStackTrace();
//                            queryPolicyRunTimes.put(file.getName(), PolicyConstants.MAX_DURATION.toString());
//                            queryRunTimes.put(file.getName(), PolicyConstants.MAX_DURATION.toString());
//                        }
//                    }
//                    writer.appendToCSVReport(queryRunTimes, policyDir, exptResultsFile);
//                    writer.appendToCSVReport(queryPolicyRunTimes, policyDir, exptResultsFile);
//                }
            }
        }
    }


    public static void main(String args[]) {
        PolicyExecutionWithQueries peq = new PolicyExecutionWithQueries();
        boolean [] templates = {true, true, true, false};
        int numOfQueries = 3;
        peq.runBEPolicies(PolicyConstants.BE_POLICY_DIR, templates, numOfQueries);
    }
}
