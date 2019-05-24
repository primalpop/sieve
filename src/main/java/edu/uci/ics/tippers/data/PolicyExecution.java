package edu.uci.ics.tippers.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.db.MySQLResult;
import edu.uci.ics.tippers.fileop.Reader;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.model.guard.FactorExtension;
import edu.uci.ics.tippers.model.guard.FactorSearch;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * Created by cygnus on 12/12/17.
 */
public class PolicyExecution {

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static final int[] policyNumbers = {100, 500, 1000, 2000, 5000, 10000, 25000, 50000};

    private static final int[] policyEpochs = {0};

    private static PolicyGeneration policyGen;

    private final ObjectMapper objectMapper = new ObjectMapper();

    Writer writer;

    MySQLQueryManager mySQLQueryManager;


    public PolicyExecution() {
        policyGen = new PolicyGeneration();
        writer = new Writer();
        objectMapper.setDateFormat(sdf);
        mySQLQueryManager = new MySQLQueryManager();
    }


    private List <Integer> generateDepths(int numOfPolicies){
        List <Integer> depths = new ArrayList<>();
        for (int i = 2; i < 10; i++) {
            depths.add((int) Math.round(Math.log(numOfPolicies)/Math.log(i)));
        }
        Set<Integer> set = new LinkedHashSet<>();
        set.addAll(depths);
        depths.clear();
        depths.addAll(set);
        return depths;
    }



    //TODO: Fix the result checker by passing the fileName to write to
    public void runBEPolicies(String policyDir) {

        //Filtering out only json files
        File dir = new File(policyDir);
        File[] policyFiles = dir.listFiles((dir1, name) -> name.toLowerCase().endsWith(".json"));
        Arrays.sort(policyFiles != null ? policyFiles : new File[0]);

        String exptResultsFile = "results.csv";
        try {
            Files.delete(Paths.get(policyDir + exptResultsFile));
        } catch (IOException ioException) { }

        boolean extension = false;

        if (policyFiles != null) {
            for (File file : policyFiles) {
                TreeMap<String, String> policyRunTimes = new TreeMap<>();
                int numOfPolicies = Integer.parseInt(file.getName().replaceAll("\\D+",""));
                System.out.println(numOfPolicies + " being processed......");
                BEExpression beExpression = new BEExpression();
                beExpression.parseJSONList(Reader.readTxt(policyDir + file.getName()));
                Duration runTime = Duration.ofMillis(0);
                boolean resultCheck = true;
                MySQLResult tradResult;
                int numOfRepetitions = 1;

                try {
                    /** Traditional approach **/
//                    System.out.println(beExpression.createQueryFromPolices());
//                    tradResult = mySQLQueryManager.runTimedQueryWithRepetitions(beExpression.createQueryFromPolices(), resultCheck, numOfRepetitions);
//                    runTime = runTime.plus(tradResult.getTimeTaken());
//
//                    policyRunTimes.put(file.getName(), String.valueOf(runTime.toMillis()));
//                    if(!(runTime.toMillis() == PolicyConstants.MAX_DURATION.toMillis())) resultCheck = true;
//                    System.out.println("** " + file.getName() + " completed and took " + runTime.toMillis());


                    List<Integer> depths = generateDepths(numOfPolicies);
                    for (int i = 0; i < depths.size(); i++) {
                        Duration guardGen = Duration.ofMillis(0);
                        Instant fsStart = Instant.now();
                        if(extension){
                            FactorExtension f = new FactorExtension(beExpression);
                            f.doYourThing();
                        }
                        FactorSearch fs = new FactorSearch(beExpression);
                        fs.search(depths.get(i));
                        Instant fsEnd = Instant.now();
                        guardGen = guardGen.plus(Duration.between(fsStart, fsEnd));
                        policyRunTimes.put(file.getName() + "-depth" + depths.get(i) + "-guardGeneration", String.valueOf(guardGen.toMillis()));
                        int numberOfGuards = fs.createQueryMap().keySet().size();
                        policyRunTimes.put(file.getName() + "-depth-NumberOfGuards" + depths.get(i), String.valueOf(numberOfGuards));
                        Duration execTime = Duration.ofMillis(0);
                        MySQLResult execResult = mySQLQueryManager.
                                runTimedQueryExp(fs.createGuardedExpQuery(false));
                        execTime = execTime.plus(execResult.getTimeTaken());
                        policyRunTimes.put(file.getName() + "-depth" + depths.get(i), String.valueOf(execTime.toMillis()));
                        writer.appendToCSVReport(policyRunTimes, policyDir, exptResultsFile);
                        policyRunTimes.clear();
                    }

                    /** Result checking **/ //TODO: Include as part of Query Manager
//                    if(resultCheck){
//                        System.out.println("Verifying results......");
//                        System.out.println("Guard query: " + fs.createCompleteQuery());
//                        MySQLResult guardResult = mySQLQueryManager.runTimedQueryWithRepetitions(fs.createCompleteQuery(),true, numOfRepetitions);
//                        Boolean resultSame = tradResult.checkResults(guardResult);
//                        if(!resultSame){
//                            System.out.println("*** Query results don't match with generated guard!!! Halting Execution ***");
//                            policyRunTimes.put(file.getName() + "-results-incorrect", String.valueOf(PolicyConstants.MAX_DURATION.toMillis()));
//                            return;
//                        }
//                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    policyRunTimes.put(file.getName(), PolicyConstants.MAX_DURATION.toString());
                }
            }
        }
    }

    //Only handles BEPolicies
    private void generatePolicies(String policyDir) {
        System.out.println("Generating Policies ..........");
        List<BEPolicy> bePolicies = new ArrayList<>();
        for (int i = 0; i < policyNumbers.length; i++) {
            List<String> attributes = new ArrayList<>();
            attributes.add(PolicyConstants.TIMESTAMP_ATTR);
            attributes.add(PolicyConstants.ENERGY_ATTR);
            attributes.add(PolicyConstants.TEMPERATURE_ATTR);
            attributes.add(PolicyConstants.LOCATIONID_ATTR);
            attributes.add(PolicyConstants.USERID_ATTR);
            attributes.add(PolicyConstants.ACTIVITY_ATTR);
            List<BEPolicy> genPolicy = policyGen.generateOverlappingPolicies(policyNumbers[i], 0.3, attributes, bePolicies);
            bePolicies.clear();
            bePolicies.addAll(genPolicy);
        }
    }

    private void persistPolicies(int numOfPolicies) {
        System.out.println("Generating Policies ..........");
        List<String> attributes = new ArrayList<>();
        attributes.add(PolicyConstants.TIMESTAMP_ATTR);
        attributes.add(PolicyConstants.ENERGY_ATTR);
        attributes.add(PolicyConstants.TEMPERATURE_ATTR);
        attributes.add(PolicyConstants.LOCATIONID_ATTR);
        attributes.add(PolicyConstants.USERID_ATTR);
        attributes.add(PolicyConstants.ACTIVITY_ATTR);
        policyGen.persistOverlappingPolicies(numOfPolicies, 0.3, attributes);
    }



    public static void main(String args[]) {
        PolicyExecution pe = new PolicyExecution();
//        pe.persistPolicies(100);
//        pe.generatePolicies(PolicyConstants.BE_POLICY_DIR);
        pe.runBEPolicies(PolicyConstants.BE_POLICY_DIR);
    }
}