package edu.uci.ics.tippers.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.fileop.Reader;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.model.guard.FactorExtension;
import edu.uci.ics.tippers.model.guard.FactorSelection;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;

import java.io.File;
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

    private long timeout = 250000;

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private Connection connection;

    private static final int[] policyNumbers = {114};

    private static PolicyGeneration policyGen;

    private final ObjectMapper objectMapper = new ObjectMapper();

    Writer writer;

    MySQLQueryManager mySQLQueryManager;


    public PolicyExecution() {
        this.connection = MySQLConnectionManager.getInstance().getConnection();
        policyGen = new PolicyGeneration();
        writer = new Writer();
        objectMapper.setDateFormat(sdf);
        mySQLQueryManager = new MySQLQueryManager();
    }


    //TODO: Fix the result checker by passing the fileName to write to
    public Map<String, Duration> runBEPolicies(String policyDir) {

        TreeMap<String, Duration> policyRunTimes = new TreeMap<>();

        File[] policyFiles = new File(policyDir).listFiles();

        Boolean resultsChecked = false;

        for (File file : policyFiles) {

            System.out.println(file.getName() + " being processed........");

            String results_file = new SimpleDateFormat("yyyyMMddHHmm").format(new Date());

            BEExpression beExpression = new BEExpression();

            beExpression.parseJSONList(Reader.readTxt(policyDir + file.getName()));

            Duration runTime = Duration.ofMillis(0);

//            System.out.println("Number Of Predicates before extension: " + beExpression.countNumberOfPredicates());

            try {
                /** Traditional approach **/
                System.out.println(beExpression.createQueryFromPolices());
                runTime = runTime.plus(mySQLQueryManager.runTimedQuery(beExpression.createQueryFromPolices(),
                        PolicyConstants.QUERY_RESULTS_DIR, results_file));
                policyRunTimes.put(file.getName(), runTime);
                System.out.println(file.getName() + " completed and took " + runTime);

                System.out.println("Starting Factor Extension");
                Duration guardGen = Duration.ofMillis(0);
                /** Extension **/
                FactorExtension f = new FactorExtension(beExpression);
                Instant feStart = Instant.now();
                f.doYourThing();
                Instant feEnd = Instant.now();
                guardGen = guardGen.plus(Duration.between(feStart, feEnd));

                /** Result checking **/
//                mySQLQueryManager.runTimedQuery(f.getGenExpression().createQueryFromPolices(),
//                        PolicyConstants.QR_EXTENDED, results_file);
//                resultsChecked = mySQLQueryManager.checkResults(PolicyConstants.QR_EXTENDED, results_file);
//                if(!resultsChecked){
//                    System.out.println("*** Query results don't match after Extension ***!!!");
//                    policyRunTimes.put(file.getName() + "-af-invalid", PolicyConstants.MAX_DURATION);
//                }

                System.out.println("Starting Factorization");
                /** Factorization **/
                FactorSelection gf = new FactorSelection(f.getGenExpression());
                Instant fsStart = Instant.now();
                gf.selectGuards();
                Instant fsEnd = Instant.now();
                guardGen = guardGen.plus(Duration.between(fsStart, fsEnd));
//                System.out.println("Factorized query " + gf.createQueryFromExactFactor());
//                runTime = Duration.ofMillis(0);
//                runTime = runTime.plus(mySQLQueryManager.runTimedQuery(gf.createQueryFromExactFactor(),
//                        PolicyConstants.QR_FACTORIZED, results_file));
                policyRunTimes.put(file.getName() + "-guardGeneration", guardGen);
                Duration guardRunTime = gf.computeGuardCosts();
                policyRunTimes.put(file.getName() + "-withGuard", guardRunTime);
                System.out.println("Number of index filters : " + gf.getIndexFilters().size());
//                System.out.println("** Factorized query took " + runTime + " **");


            } catch (Exception e) {
                e.printStackTrace();
                policyRunTimes.put(file.getName(), PolicyConstants.MAX_DURATION);
            }
        }

        return policyRunTimes;
    }

    //Only handles BEPolicies
    private void generatePolicies(String policyDir) {
        System.out.println("Generating Policies ..........");
        List<BEPolicy> bePolicies = new ArrayList<>();
        for (int i = 0; i < policyNumbers.length; i++) {
            List<String> attributes = new ArrayList<>();
            attributes.add(PolicyConstants.TIMESTAMP_ATTR);
            attributes.add(PolicyConstants.USERID_ATTR);
            attributes.add(PolicyConstants.ENERGY_ATTR);
            attributes.add(PolicyConstants.TEMPERATURE_ATTR);
            attributes.add(PolicyConstants.LOCATIONID_ATTR);
            List<BEPolicy> genPolicy = policyGen.generateFilteredBEPolicy(policyNumbers[i], attributes, bePolicies);
            bePolicies.clear();
            bePolicies.addAll(genPolicy);
        }
    }

    private void bePolicyExperiments(String policyDir) {
        TreeMap<String, Duration> runTimes = new TreeMap<>();
        runTimes.putAll(runBEPolicies(policyDir));
        writer.createTextReport(runTimes, policyDir);
    }


    public static void main(String args[]) {
        PolicyExecution pe = new PolicyExecution();
//        pe.generatePolicies(PolicyConstants.BE_POLICY_DIR);
        pe.bePolicyExperiments(PolicyConstants.BE_POLICY_DIR);
    }
}
