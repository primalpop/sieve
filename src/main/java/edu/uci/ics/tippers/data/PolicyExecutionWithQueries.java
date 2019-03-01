package edu.uci.ics.tippers.data;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.db.MySQLResult;
import edu.uci.ics.tippers.fileop.Reader;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.model.guard.FactorExtension;
import edu.uci.ics.tippers.model.guard.FactorSearch;
import edu.uci.ics.tippers.model.policy.BEExpression;

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

    public PolicyExecutionWithQueries() {
        writer = new Writer();
        mySQLQueryManager = new MySQLQueryManager();
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

        //Generating the queries based on templates
        QueryGeneration qg = new QueryGeneration();
        List<String> queries = null;
        try {
            queries = Files.readAllLines(Paths.get(PolicyConstants.BE_POLICY_DIR + PolicyConstants.QUERY_FILE));
        } catch (IOException e) {
            e.printStackTrace();
        }


        if (policyFiles != null) {
            for (File file : policyFiles) {
                TreeMap<String, String> policyRunTimes = new TreeMap<>();
                TreeMap<String,String> queryRunTimes = new TreeMap<>(Comparator.comparingInt(Integer::parseInt));
                System.out.println(file.getName() + " being processed......");
                BEExpression beExpression = new BEExpression();
                beExpression.parseJSONList(Reader.readTxt(policyDir + file.getName()));
                Duration runTime = Duration.ofMillis(0);
                boolean resultCheck = false;
                MySQLResult tradResult;
                int numOfRepetitions = 3;

                MySQLResult policyResult = mySQLQueryManager.runTimedQuery(beExpression.createQueryFromPolices(), resultCheck, numOfRepetitions);
                System.out.println("Policy only: " + policyResult.getTimeTaken());
//                System.out.println(beExpression.createQueryFromPolices());

                System.out.println("Starting Generation......");
                Duration guardGen = Duration.ofMillis(0);
                FactorExtension f = new FactorExtension(beExpression);
                Instant feStart = Instant.now();
                int ext = f.doYourThing();
                policyRunTimes.put("Number of Extensions", String.valueOf(ext));
                Instant feEnd = Instant.now();
                guardGen = guardGen.plus(Duration.between(feStart, feEnd));

                FactorSearch fs = new FactorSearch(beExpression);
                Instant fsStart = Instant.now();
                fs.search();
                Instant fsEnd = Instant.now();
                guardGen = guardGen.plus(Duration.between(fsStart, fsEnd));
                writer.addGuardReport(fs.printDetailedResults(numOfRepetitions), policyDir, exptResultsFile);
                policyRunTimes.put(file.getName() + "-guardGeneration", String.valueOf(guardGen.toMillis()));
                writer.appendToCSVReport(policyRunTimes, policyDir, exptResultsFile);
                int queryCount = 1;
                for (String query: queries) {
                    try {
                        /** Traditional approach **/
                        String tradQuery = "(" + query + ") AND (" + beExpression.createQueryFromPolices() + ")";
                        tradResult = mySQLQueryManager.runTimedQuery(tradQuery, resultCheck, numOfRepetitions);
                        runTime = runTime.plus(tradResult.getTimeTaken());
                        queryRunTimes.put(String.valueOf(queryCount), String.valueOf(runTime.toMillis()));
                        if (!(runTime.toMillis() == PolicyConstants.MAX_DURATION.toMillis())) resultCheck = true;
                        System.out.println("** " + file.getName() + " " + queryCount + " completed and took " + runTime.toMillis());
                        MySQLResult queryResult = mySQLQueryManager.runTimedQuery(query, resultCheck, numOfRepetitions);
                        queryRunTimes.put(String.valueOf(queryCount), String.valueOf(queryResult.getTimeTaken().toMillis()));
                        queryCount+= 1;
                    }catch (Exception e) {
                        e.printStackTrace();
                        queryRunTimes.put(file.getName(), PolicyConstants.MAX_DURATION.toString());
                    }
                }
                writer.appendToCSVReport(queryRunTimes, policyDir, exptResultsFile);
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
