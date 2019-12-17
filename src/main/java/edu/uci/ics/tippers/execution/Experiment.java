package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.db.MySQLResult;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.generation.policy.PolicyGen;
import edu.uci.ics.tippers.manager.GuardPersistor;
import edu.uci.ics.tippers.manager.PolicyPersistor;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.model.guard.GuardPart;
import edu.uci.ics.tippers.model.guard.SelectGuard;
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

    private static boolean BASE_LINE;
    private static boolean GENERATE_GUARD;
    private static boolean GUARD_EXEC;
    private static boolean UDF_EXEC;
    private static boolean HYBRID_EXEC;
    private static boolean RESULT_CHECK;

    private static boolean EXTEND_PREDICATES;

    private static boolean GUARD_UNION;
    private static int NUM_OF_REPS;
    private static long QUERY_TIMEOUT;

    private static String RESULTS_FILE;

    public Experiment() {
        polper = new PolicyPersistor();
        mySQLQueryManager = new MySQLQueryManager();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("config/basic.properties");
            Properties props = new Properties();
            if (inputStream != null) {
                props.load(inputStream);
                BASE_LINE = Boolean.parseBoolean(props.getProperty("baseline"));
                GENERATE_GUARD = Boolean.parseBoolean(props.getProperty("generate_guard"));
                GUARD_EXEC = Boolean.parseBoolean(props.getProperty("guard_exec"));
                UDF_EXEC = Boolean.parseBoolean(props.getProperty("udf_exec"));
                HYBRID_EXEC = Boolean.parseBoolean(props.getProperty("hybrid_exec"));
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


    public String runBEPolicies(String querier, List<BEPolicy> bePolicies) {


        //TODO: to replace with queries generated from templates
        String QUERY_PREDICATES = " location_id in ('3141-clwa-1412', '3145-clwa-5099', '3143-clwa-3059') " +
                "and start_date >= '2018-02-01' and start_date <= '2018-02-15' and start_time >= '08:04:51' " +
                "and start_time <= '23:54:51' and user_id = 1628";

        BEExpression beExpression = new BEExpression(bePolicies);
        StringBuilder resultString = new StringBuilder();
        resultString.append(querier).append(",").append(bePolicies.size()).append(",");
        try {

            MySQLResult tradResult = new MySQLResult();
            //TODO: fix baseline query
            if (BASE_LINE) {
                tradResult = mySQLQueryManager.runTimedQueryWithRepetitions(beExpression.createQueryFromPolices(),
                        RESULT_CHECK, NUM_OF_REPS);
                Duration runTime = Duration.ofMillis(0);
                runTime = runTime.plus(tradResult.getTimeTaken());
                resultString.append(runTime.toMillis()).append(",");
                System.out.println("QW: No of Policies: " + beExpression.getPolicies().size() + " , Time: " + runTime.toMillis());
            }

            /** Guard generation **/
            if (GENERATE_GUARD) { //TODO: don't generate guard, retrieve it instead
                Duration guardGen = Duration.ofMillis(0);
                Instant fsStart = Instant.now();
                SelectGuard gh = new SelectGuard(beExpression, EXTEND_PREDICATES);
                Instant fsEnd = Instant.now();
                guardGen = guardGen.plus(Duration.between(fsStart, fsEnd));
                System.out.println("Guard Generation: " + guardGen + " Number of Guards: " + gh.numberOfGuards());
                resultString.append(gh.numberOfGuards()).append(",");
                /** Result checking **/
                if (RESULT_CHECK) {
                    System.out.println("Verifying results......");
                    System.out.println("Guard query: " + gh.createGuardedQuery(false));
                    MySQLResult guardResult = mySQLQueryManager.runTimedSubQuery(gh.createGuardedQuery(false),
                            RESULT_CHECK);
                    Boolean resultSame = tradResult.checkResults(guardResult);
                    if (!resultSame) {
                        System.out.println("*** Query results don't match with generated guard!!! Halting Execution ***");
                        return null;
                    }
                }

                if (GUARD_EXEC) {
                    String guard_query = "SELECT * FROM (" + gh.createGuardedQuery(GUARD_UNION) + ") as P where " + QUERY_PREDICATES;
                    Duration execTime = Duration.ofMillis(0);
                    MySQLResult execResult = mySQLQueryManager.runTimedQueryExp(guard_query);
                    execTime = execTime.plus(execResult.getTimeTaken());
                    resultString.append(execTime.toMillis()).append(",");
                    System.out.println("Guard execution: " + " Time: " + execTime.toMillis());
                }
            }
            if(UDF_EXEC){
                Duration execTime = Duration.ofMillis(0);
                String udf_query = "SELECT * FROM PRESENCE as p where " + QUERY_PREDICATES
                        + " and pcheck( " + querier + ", p.user_id, p.location_id, " +
                        "p.start_date, p.start_time, p.user_profile, p.user_group) = 1";
                MySQLResult execResult = mySQLQueryManager.runTimedQueryExp(udf_query);
                execTime = execTime.plus(execResult.getTimeTaken());
                resultString.append(execTime.toMillis()).append(",");
                System.out.println("UDF execution: "  + " Time: " + execTime.toMillis());
            }
            if(HYBRID_EXEC){
                Duration execTime = Duration.ofMillis(0);
                GuardPersistor guardPersistor = new GuardPersistor();
                GuardExp guardExp = guardPersistor.retrieveGuard(querier, "user");
                StringBuilder hybrid_query = new StringBuilder();
                String delim = "";
                for(GuardPart guardPart: guardExp.getGuardParts()){
                    hybrid_query.append(delim)
                            .append("SELECT * from PRESENCE as p where ")
                            .append(guardPart.getGuard().print())
                            .append(PolicyConstants.CONJUNCTION)
                            .append(" hybcheck(").append(querier).append(", \'")
                            .append(guardPart.getId()).append("\', ")
                            .append("p.user_id, p.location_id, p.start_date, p.start_time, p.user_profile, p.user_group ) = 1 ")
                            .append(PolicyConstants.CONJUNCTION)
                            .append(QUERY_PREDICATES);
                    delim = PolicyConstants.UNION;
                }
                MySQLResult execResult = mySQLQueryManager.runTimedQueryExp(hybrid_query.toString());
                execTime = execTime.plus(execResult.getTimeTaken());
                resultString.append(execTime.toMillis()).append("\n");
                System.out.println("Hybrid execution: "  + " Time: " + execTime.toMillis());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultString.toString();
    }

    public static void main(String[] args) {
        Experiment e = new Experiment();
        PolicyGen pg = new PolicyGen();
        List<Integer> users = pg.getAllUsers(true);
        PolicyPersistor polper = new PolicyPersistor();
        String file_header = "Querier,Number_Of_Policies,Baseline,Number_Of_Guards,Guard,UDF,Hybrid \n";
        Writer writer = new Writer();
        writer.writeString(file_header, PolicyConstants.BE_POLICY_DIR, RESULTS_FILE);
        for (int i = 0; i < 10; i++) {
            String querier = String.valueOf(users.get(i));
            List<BEPolicy> allowPolicies = polper.retrievePolicies(querier,
                    PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
            if(allowPolicies == null) continue;
            System.out.println("Querier " + querier);
            writer.writeString(e.runBEPolicies(querier, allowPolicies), PolicyConstants.BE_POLICY_DIR, RESULTS_FILE);
        }
    }
}
