package edu.uci.ics.tippers.data;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.fileop.Reader;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.model.guard.FactorSelection;
import edu.uci.ics.tippers.model.guard.PredicateExtension;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.query.BasicQuery;
import edu.uci.ics.tippers.model.query.RangeQuery;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.Date;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by cygnus on 12/12/17.
 */
public class PolicyExecution {

    private long timeout = 250000;

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private Connection connection;

    private static final int[] policyNumbers = {5, 10};

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

        Map<String, Duration> policyRunTimes = new HashMap<>();

        File[] policyFiles = new File(policyDir).listFiles();

        Boolean resultsChecked = false;

        for (File file : policyFiles) {

            System.out.println(file.getName() + " being processed........");

            String results_file = new SimpleDateFormat("yyyyMMddHHmm").format(new Date());

            BEExpression beExpression = new BEExpression();

            beExpression.parseJSONList(Reader.readTxt(policyDir + file.getName()));

            Duration runTime = Duration.ofMillis(0);

            System.out.println("Number Of Predicates before extension: " + beExpression.countNumberOfPredicates());

            try {
                /** Traditional approach **/
                System.out.println(beExpression.createQueryFromPolices());
                runTime = runTime.plus(mySQLQueryManager.runTimedQuery(beExpression.createQueryFromPolices(),
                        PolicyConstants.QUERY_RESULTS_DIR, results_file));
                policyRunTimes.put(file.getName(), runTime);
                System.out.println(file.getName() + " completed and took " + runTime);

                /** Factorization **/
                FactorSelection gf = new FactorSelection(beExpression);
//                Instant sG = Instant.now();
                gf.selectFactor();
//                Instant eG = Instant.now();
                System.out.println(gf.createQueryFromExactFactor());

//                System.out.println("Factorization took " + Duration.between(sG, eG));
                System.out.println("Factorized query " + gf.createQueryFromExactFactor());
                runTime = Duration.ofMillis(0);
                runTime = runTime.plus(mySQLQueryManager.runTimedQuery(gf.createQueryFromExactFactor(),
                        PolicyConstants.QR_FACTORIZED, results_file));
                policyRunTimes.put(file.getName() + "-gf", runTime);
                System.out.println("** Factorized query took " + runTime + " **");


                /** Extension **/
                runTime = Duration.ofMillis(0);
                PredicateExtension f = new PredicateExtension(gf);
//                Instant sG = Instant.now();
                f.extendPredicate();
//                Instant eG = Instant.now();
//                System.out.println("Factorization took " + Duration.between(sG, eG));

                mySQLQueryManager.runTimedQuery(f.printGuardMap(), PolicyConstants.QR_EXTENDED, results_file);

                /** Result checking **/
                resultsChecked = mySQLQueryManager.checkResults(PolicyConstants.QR_EXTENDED, results_file);
                if(!resultsChecked){
                    System.out.println("*** Query results don't match after Extension ***!!!");
                    policyRunTimes.put(file.getName() + "-af-invalid", PolicyConstants.MAX_DURATION);
                }
                else {
                    Duration guardExecTime = f.computeGuardCosts();
                    System.out.println("Guard Rep cost: " + guardExecTime);
                    policyRunTimes.put(file.getName() + "-pe", guardExecTime);
                }
            } catch (Exception e) {
                e.printStackTrace();
                policyRunTimes.put(file.getName(), PolicyConstants.MAX_DURATION);
            }
        }

        return policyRunTimes;
    }

    //Only handles BEPolicies
    private void generatePolicies(String policyDir) {
        List<BEPolicy> bePolicies = new ArrayList<>();
        for (int i = 0; i < policyNumbers.length; i++) {
            List<String> attributes = new ArrayList<>();
            attributes.add(PolicyConstants.TIMESTAMP_ATTR);
            attributes.add(PolicyConstants.USERID_ATTR);
            attributes.add(PolicyConstants.ENERGY_ATTR);
            attributes.add(PolicyConstants.TEMPERATURE_ATTR);
            attributes.add(PolicyConstants.LOCATIONID_ATTR);
            List<BEPolicy> genPolicy = policyGen.generateBEPolicy(policyNumbers[i], attributes, bePolicies);
            bePolicies.clear();
            bePolicies.addAll(genPolicy);
        }
    }

    private void bePolicyExperiments(String policyDir) {
        Map<String, Duration> runTimes = new HashMap<>();
        runTimes.putAll(runBEPolicies(policyDir));
        writer.createTextReport(runTimes, policyDir);
    }


    public static void main(String args[]) {
        PolicyExecution pe = new PolicyExecution();
//        pe.generatePolicies(PolicyConstants.BE_POLICY_DIR);

        pe.bePolicyExperiments(PolicyConstants.BE_POLICY_DIR);
    }
}
