package edu.uci.ics.tippers.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.db.MySQLResult;
import edu.uci.ics.tippers.fileop.Reader;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.model.guard.FactorExtension;
import edu.uci.ics.tippers.model.guard.FactorSelection;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.Date;

/**
 * Created by cygnus on 12/12/17.
 */
public class PolicyExecution {

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static final int[] policyNumbers = {35};

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


    //TODO: Fix the result checker by passing the fileName to write to
    public void runBEPolicies(String policyDir) {

        TreeMap<String, String> policyRunTimes = new TreeMap<>();
        //Filtering out only json files
        File dir = new File(policyDir);
        File[] policyFiles = dir.listFiles((dir1, name) -> name.toLowerCase().endsWith(".json"));
        Arrays.sort(policyFiles != null ? policyFiles : new File[0]);

        String exptResults = "results.csv";
        try {
            Files.delete(Paths.get(policyDir + exptResults));
        } catch (IOException ioException) {
            System.out.println("Error caught in deleting existing results file");
            ioException.printStackTrace();
        }

        if (policyFiles != null) {
            for (File file : policyFiles) {

                System.out.println(file.getName() + " being processed......");

                BEExpression beExpression = new BEExpression();

                beExpression.parseJSONList(Reader.readTxt(policyDir + file.getName()));

                Duration runTime = Duration.ofMillis(0);

                Boolean resultCheck = false;

                MySQLResult tradResult = new MySQLResult();

    //            System.out.println("Number Of Predicates before extension: " + beExpression.countNumberOfPredicates());

                try {
                    /** Traditional approach **/
                    System.out.println(beExpression.createQueryFromPolices());
                    tradResult = mySQLQueryManager.runTimedQuery(beExpression.createQueryFromPolices(), true);
                    runTime = runTime.plus(tradResult.getTimeTaken());

                    policyRunTimes.put(file.getName(), String.valueOf(runTime.toMillis()));
                    if(!(runTime.toMillis() == PolicyConstants.MAX_DURATION.toMillis())) resultCheck = true;
                    System.out.println("** " + file.getName() + " completed and took " + runTime.toMillis());

                    System.out.println("Starting Generation......");
                    Duration guardGen = Duration.ofMillis(0);
    //                /** Extension **/
                    FactorExtension f = new FactorExtension(beExpression);
                    Instant feStart = Instant.now();
                    int ext = f.doYourThing();
                    policyRunTimes.put("Number of Extensions", String.valueOf(ext));
                    Instant feEnd = Instant.now();
                    guardGen = guardGen.plus(Duration.between(feStart, feEnd));

                    /** Result checking after factor extension **/
    //                System.out.println("Verifying results after factor extension......");
    //                System.out.println("Intermediate query: " + f.getGenExpression().createQueryFromPolices());
    //                MySQLResult interResult = mySQLQueryManager.runTimedQuery(f.getGenExpression().createQueryFromPolices(),resultCheck);
    //                Boolean interSame = tradResult.checkResults(interResult);
    //                if(!interSame){
    //                    System.out.println("*** Query results don't match after Extension!!! Halting Execution ***");
    //                    policyRunTimes.put(file.getName() + "-results-incorrect", String.valueOf(PolicyConstants.MAX_DURATION.toMillis()));
    //                    return;
    //                }


    //                System.out.println("Starting Factorization");
                    /** Factorization **/
                    FactorSelection gf = new FactorSelection(f.getGenExpression());
                    Instant fsStart = Instant.now();
                    gf.selectGuards();
                    Instant fsEnd = Instant.now();


                    /** Result checking **/
    //                if(resultCheck){
    //                    System.out.println("Verifying results......");
    //                    System.out.println("Guard query: " + gf.createQueryFromExactFactor());
    //                    MySQLResult guardResult = mySQLQueryManager.runTimedQuery(gf.createQueryFromExactFactor(),true);
    //                    Boolean resultSame = tradResult.checkResults(guardResult);
    //                    if(!resultSame){
    //                        System.out.println("*** Query results don't match with generated guard!!! Halting Execution ***");
    //                        policyRunTimes.put(file.getName() + "-results-incorrect", String.valueOf(PolicyConstants.MAX_DURATION.toMillis()));
    //                        return;
    //                    }
    //                }


                    guardGen = guardGen.plus(Duration.between(fsStart, fsEnd));
    //                System.out.println("Final Guard " + gf.createQueryFromExactFactor());
                    System.out.println("Policies "+ file.getName() + " Guard Generation time: " + guardGen.toMillis());
                    policyRunTimes.put(file.getName() + "-guardGeneration", String.valueOf(guardGen.toMillis()));
                    policyRunTimes.put("Number of original guards ", String.valueOf(gf.getIndexFilters().size()));

                    writer.appendToCSVReport(policyRunTimes, policyDir, exptResults);
                    policyRunTimes.clear();
                    System.out.println("Starting Execution of Guard for " + file.getName() + " ......");
                    List<String> guardResults = gf.printDetailedGuardResults();
                    gf.createDirtyQuery();
                    writer.addGuardReport(guardResults, policyDir, exptResults);
    //                Duration guardRunTime = gf.computeGuardCosts();
    //                policyRunTimes.put(file.getName() + "-withGuard", guardRunTime);
    //                System.out.println("** Number of index filters : " + gf.getIndexFilters().size() );
    //                System.out.println("** Guard query for " + file.getName() + " took " + guardRunTime.toMillis() + " **");

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
            List<BEPolicy> genPolicy = policyGen.generateOverlappingPolicies(policyNumbers[i], 0.4, attributes, bePolicies);
            bePolicies.clear();
            bePolicies.addAll(genPolicy);
        }
    }


    public static void main(String args[]) {
        PolicyExecution pe = new PolicyExecution();
//        pe.generatePolicies(PolicyConstants.BE_POLICY_DIR);
        pe.runBEPolicies(PolicyConstants.BE_POLICY_DIR);
    }
}