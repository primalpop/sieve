package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.db.MySQLResult;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.manager.PolicyPersistor;
import edu.uci.ics.tippers.model.guard.GuardHit;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

public class Experiment {


    /**
     * 1) Retrieve policies - Change the code to retrieve list of policies
     * 2) Create Policy Expression
     * 3) Generate guard based on the policy
     * 4) Execute guards as a union on top of the database
     */

    PolicyPersistor polper;
    MySQLQueryManager mySQLQueryManager;


    private Writer writer;

    private static boolean BASE_LINE;
    private static boolean RESULT_CHECK;

    private static boolean EXTEND_PREDICATES;

    private static boolean GUARD_UNION;
    private static int NUM_OF_REPS;
    private static long QUERY_TIMEOUT;

    private static String RESULTS_FILE;

    public Experiment() {
        polper = new PolicyPersistor();
        mySQLQueryManager = new MySQLQueryManager();
        writer = new Writer();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("config/basic.properties");
            Properties props = new Properties();
            if (inputStream != null) {
                props.load(inputStream);
                BASE_LINE = Boolean.parseBoolean(props.getProperty("baseline"));
                RESULT_CHECK = Boolean.parseBoolean(props.getProperty("resultCheck"));
                EXTEND_PREDICATES = Boolean.parseBoolean(props.getProperty("factor_extension"));
                GUARD_UNION = Boolean.parseBoolean(props.getProperty("union"));
                NUM_OF_REPS = Integer.parseInt(props.getProperty("numberOfRepetitions"));
                QUERY_TIMEOUT = Long.parseLong(props.getProperty("timeout"));
                RESULTS_FILE = props.getProperty("results_file");
            }

        } catch (IOException ie) {
            ie.printStackTrace();
        }
    }


    public void runBEPolicies() {

        List<BEPolicy> policies = polper.retrievePolicies("10", "user");
        BEExpression beExpression = new BEExpression(policies);
        System.out.println("Original Policies: " + beExpression.createQueryFromPolices());

        try {

            MySQLResult tradResult = new MySQLResult();

            if (BASE_LINE) {
                tradResult = mySQLQueryManager.runTimedQueryWithRepetitions(beExpression.createQueryFromPolices(),
                        RESULT_CHECK, NUM_OF_REPS);
                Duration runTime = Duration.ofMillis(0);
                runTime = runTime.plus(tradResult.getTimeTaken());
                System.out.println("Baseline approach for " + beExpression.getPolicies().size() + " policies took " + runTime.toMillis());
            }

            Duration guardGen = Duration.ofMillis(0);
            Instant fsStart = Instant.now();
            GuardHit gh = new GuardHit(beExpression, EXTEND_PREDICATES);
            Instant fsEnd = Instant.now();
            guardGen = guardGen.plus(Duration.between(fsStart, fsEnd));
            System.out.println("Guard Generation time: " + guardGen + " Number of Guards: " + gh.numberOfGuards());

            /** Result checking **/
            if (RESULT_CHECK) {
                System.out.println("Verifying results......");
                System.out.println("Guard query: " + gh.createGuardedQuery(false));
                MySQLResult guardResult = mySQLQueryManager.runTimedSubQuery(gh.createGuardedQuery(false),
                        RESULT_CHECK);
                Boolean resultSame = tradResult.checkResults(guardResult);
                if (!resultSame) {
                    System.out.println("*** Query results don't match with generated guard!!! Halting Execution ***");
                    return;
                }
            }

            Duration execTime = Duration.ofMillis(0);
            MySQLResult execResult = mySQLQueryManager.runTimedQueryExp(gh.createGuardedQuery(GUARD_UNION));
            execTime = execTime.plus(execResult.getTimeTaken());
            System.out.println("Guard execution with Union " + GUARD_UNION + " took " + execTime.toMillis() + " milliseconds");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]) {
        Experiment e = new Experiment();
        e.runBEPolicies();
    }
}
