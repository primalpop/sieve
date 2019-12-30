package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.db.MySQLResult;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.generation.policy.PolicyGen;
import edu.uci.ics.tippers.generation.query.QueryExplainer;
import edu.uci.ics.tippers.generation.query.QueryGeneration;
import edu.uci.ics.tippers.manager.GuardPersistor;
import edu.uci.ics.tippers.manager.PolicyPersistor;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.query.QueryStatement;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.time.Duration;
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
    QueryExplainer queryExplainer;
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
        queryExplainer = new QueryExplainer();
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


    public String runBEPolicies(String querier, String queryPredicates, List<BEPolicy> bePolicies) {

        BEExpression beExpression = new BEExpression(bePolicies);
        StringBuilder resultString = new StringBuilder();
        resultString.append(querier).append(",").append(bePolicies.size()).append(",");
        try {

            MySQLResult tradResult = new MySQLResult();
            if (BASE_LINE) {
                String baselineQuery = "With polEval as ( Select * from PRESENCE where "
                        + beExpression.createQueryFromPolices() + "  )"
                        + "Select * from polEval where " + queryPredicates;
                tradResult = mySQLQueryManager.runTimedQueryExp(baselineQuery);
                Duration runTime = Duration.ofMillis(0);
                runTime = runTime.plus(tradResult.getTimeTaken());
                resultString.append(runTime.toMillis()).append(",");
                System.out.println("QW: No of Policies: " + beExpression.getPolicies().size() + " , Time: " + runTime.toMillis());
            }


            
            GuardPersistor guardPersistor = new GuardPersistor();
            GuardExp guardExp = guardPersistor.retrieveGuardExpression(querier, "user", bePolicies);

            if(HYBRID_EXEC){
                Duration execTime = Duration.ofMillis(0);
                String hybrid_query = queryExplainer.estimateCost("Select * from PRESENCE where " + queryPredicates)
                        < guardExp.estimateCostofGuardScan()? guardExp.inlineOrNot(false) : guardExp.inlineOrNot(true);
                hybrid_query += "Select * from polEval where " + queryPredicates;
                System.out.println("Hybrid query: ");
                System.out.println(hybrid_query);
                MySQLResult execResult = mySQLQueryManager.runTimedQueryExp(hybrid_query);
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
        QueryGeneration qg = new QueryGeneration();
        List<QueryStatement> queries = qg.retrieveQueries("low", 1);
        List<Integer> users = pg.getAllUsers(true);
        PolicyPersistor polper = new PolicyPersistor();
        String file_header = "Querier,Number_Of_Policies,Baseline,Number_Of_Guards,Sieve \n";
        Writer writer = new Writer();
        writer.writeString(file_header, PolicyConstants.BE_POLICY_DIR, RESULTS_FILE);
        for (int i = 0; i < 10; i++) {
            String querier = String.valueOf(users.get(i));
            List<BEPolicy> allowPolicies = polper.retrievePolicies(querier,
                    PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
            if(allowPolicies == null) continue;
            System.out.println("Querier " + querier);
            for (int j = 0; j < queries.size(); j++) {
                writer.writeString(e.runBEPolicies(querier, queries.get(j).getQuery(), allowPolicies),
                        PolicyConstants.BE_POLICY_DIR, RESULTS_FILE);
            }
        }
    }
}
