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
import edu.uci.ics.tippers.model.guard.GuardPart;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.query.QueryStatement;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
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

    private static boolean QUERY_EXEC;
    private static boolean BASE_LINE_POLICIES;
    private static boolean BASELINE_UDF;
    private static boolean GENERATE_GUARD;
    private static boolean GUARD_POLICY_INLINE;
    private static boolean GUARD_UDF;
    private static boolean GUARD_HYBRID;
    private static boolean SIEVE_EXEC;
    private static boolean RESULT_CHECK;

    private static int NUM_OF_REPS;

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
                QUERY_EXEC = Boolean.parseBoolean(props.getProperty("query_exec"));
                BASE_LINE_POLICIES = Boolean.parseBoolean(props.getProperty("baseline_policies"));
                GENERATE_GUARD = Boolean.parseBoolean(props.getProperty("generate_guard"));
                GUARD_POLICY_INLINE = Boolean.parseBoolean(props.getProperty("guard_policies"));
                BASELINE_UDF = Boolean.parseBoolean(props.getProperty("baseline_udf"));
                GUARD_UDF = Boolean.parseBoolean(props.getProperty("guard_udf"));
                GUARD_HYBRID = Boolean.parseBoolean(props.getProperty("guard_hybrid"));
                SIEVE_EXEC = Boolean.parseBoolean(props.getProperty("sieve_exec"));
                RESULT_CHECK = Boolean.parseBoolean(props.getProperty("resultCheck"));
                NUM_OF_REPS = Integer.parseInt(props.getProperty("num_repetitions"));
                RESULTS_FILE = props.getProperty("results_file");
            }

        } catch (IOException ie) {
            ie.printStackTrace();
        }
    }


    public String hintOrNot(GuardExp guardExp, String guardQuery, String queryPredicates){
        double querySel = mySQLQueryManager.checkSelectivity(queryPredicates);
        double totalCard = guardExp.getGuardParts().stream().mapToDouble(GuardPart::getCardinality).sum();
        String sieve_query = null;
        if (querySel > totalCard) { //Use Guards
            sieve_query+=  guardQuery + "Select * from polEval where " + queryPredicates;
        }
        else { //Use queries
            String query_hint = "date_tree"; //TODO: What is the query hint to be given?
            sieve_query = "SELECT * from ( SELECT * from PRESENCE force index(" + query_hint
                    + ") where " + queryPredicates + " ) as P where " + guardExp.createQueryWithOR();
        }
        return sieve_query;
    }

    public String runBEPolicies(String querier, String queryPredicates, int template, float selectivity, List<BEPolicy> bePolicies) {

        BEExpression beExpression = new BEExpression(bePolicies);
        StringBuilder resultString = new StringBuilder();
        resultString.append(querier).append(",").append(selectivity * PolicyConstants.NUMBER_OR_TUPLES).append(",")
                .append(template).append(",")
                .append(bePolicies.size()).append(",");
        try {
            if(QUERY_EXEC){
                MySQLResult queryResult = new MySQLResult();
                String baselineQuery = " Select * from PRESENCE where " + queryPredicates;
                queryResult = mySQLQueryManager.runTimedQueryExp(baselineQuery, NUM_OF_REPS);
                Duration runTime = Duration.ofMillis(0);
                runTime = runTime.plus(queryResult.getTimeTaken());
                System.out.println("Time taken by query alone " + runTime.toMillis());
                resultString.append(runTime.toMillis()).append(",");
                mySQLQueryManager.increaseTimeout(runTime.toMillis()); //Updating timeout to query exec time + constant
            }

            if (BASE_LINE_POLICIES) {
                MySQLResult tradResult = new MySQLResult();
                String baselineQuery = "With polEval as ( Select * from PRESENCE where "
                        + beExpression.createQueryFromPolices() + "  )"
                        + "Select * from polEval where " + queryPredicates;
                tradResult = mySQLQueryManager.runTimedQueryExp(baselineQuery, NUM_OF_REPS);
                Duration runTime = Duration.ofMillis(0);
                runTime = runTime.plus(tradResult.getTimeTaken());
                resultString.append(runTime.toMillis()).append(",");
                System.out.println("Baseline inlining policies: No of Policies: " + beExpression.getPolicies().size() + " , Time: " + runTime.toMillis());
            }

            if(BASELINE_UDF){
                Duration execTime = Duration.ofMillis(0);
                String udf_query = "SELECT * FROM PRESENCE as p where " + queryPredicates
                        + " and pcheck( " + querier + ", p.user_id, p.location_id, " +
                        "p.start_date, p.start_time, p.user_profile, p.user_group) = 1";
                MySQLResult execResult = mySQLQueryManager.runTimedQueryExp(udf_query, NUM_OF_REPS);
                execTime = execTime.plus(execResult.getTimeTaken());
                resultString.append(execTime.toMillis()).append(",");
                System.out.println("UDF execution: "  + " Time: " + execTime.toMillis());
            }

            GuardPersistor guardPersistor = new GuardPersistor();
            GuardExp guardExp = guardPersistor.retrieveGuardExpression(querier, "user", bePolicies);
            if(guardExp.getGuardParts().isEmpty()) return "empty";
            resultString.append(guardExp.getGuardParts().size()).append(",");

            if(GUARD_POLICY_INLINE) {
                Duration execTime = Duration.ofMillis(0);
                String guard_query_with_union = guardExp.inlineRewrite(true);
                String guard_query_with_or = guardExp.inlineRewrite(false);
                guard_query_with_union += "Select * from polEval where " + queryPredicates;
                guard_query_with_or += "Select * from polEval where " + queryPredicates;
                MySQLResult execResult = mySQLQueryManager.runTimedQueryExp(guard_query_with_union, NUM_OF_REPS);
                execTime = execTime.plus(execResult.getTimeTaken());
                resultString.append(execTime.toMillis()).append(",");
                System.out.println("Guard inline execution with union: "  + " Time: " + execTime.toMillis());
                execResult = mySQLQueryManager.runTimedQueryExp(guard_query_with_or, NUM_OF_REPS);
                execTime = execTime.plus(execResult.getTimeTaken());
                resultString.append(execTime.toMillis()).append(",");
                System.out.println("Guard inline execution with OR: "  + " Time: " + execTime.toMillis());
            }
            if(GUARD_UDF){
                Duration execTime = Duration.ofMillis(0);
                String guard_query_with_union = guardExp.udfRewrite(true);
                String guard_query_with_or = guardExp.udfRewrite(false);
                guard_query_with_union += "Select * from polEval where " + queryPredicates;
                guard_query_with_or += "Select * from polEval where " + queryPredicates;
                MySQLResult execResult = mySQLQueryManager.runTimedQueryExp(guard_query_with_union, NUM_OF_REPS);
                execTime = execTime.plus(execResult.getTimeTaken());
                resultString.append(execTime.toMillis()).append(",");
                System.out.println("Guard udf execution with union: "  + " Time: " + execTime.toMillis());
                execResult = mySQLQueryManager.runTimedQueryExp(guard_query_with_or, NUM_OF_REPS);
                execTime = execTime.plus(execResult.getTimeTaken());
                resultString.append(execTime.toMillis());
                System.out.println("Guard udf execution with OR: "  + " Time: " + execTime.toMillis());
            }
            if(GUARD_HYBRID) {
                Duration execTime = Duration.ofMillis(0);
                String guard_hybrid_query = guardExp.inlineOrNot(true);
                guard_hybrid_query += "Select * from polEval where " + queryPredicates;
                MySQLResult execResult = mySQLQueryManager.runTimedQueryExp(guard_hybrid_query, NUM_OF_REPS);
                execTime = execTime.plus(execResult.getTimeTaken());
                resultString.append(execTime.toMillis()).append(",");
                System.out.println("Guard Hybrid execution : "  + " Time: " + execTime.toMillis());
            }
            if(SIEVE_EXEC){
                Duration execTime = Duration.ofMillis(0);
                String sieve_query = hintOrNot(guardExp, guardExp.inlineOrNot(true), queryPredicates) ;
                MySQLResult execResult = mySQLQueryManager.runTimedQueryExp(sieve_query, NUM_OF_REPS);
                execTime = execTime.plus(execResult.getTimeTaken());
                resultString.append(execTime.toMillis()).append(",");
                System.out.println("Sieve Query: "  + " Time: " + execTime.toMillis());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultString.append("\n").toString();
    }

    public List<QueryStatement> getQueries(int template, int query_count){
        QueryGeneration qg = new QueryGeneration();
        List<QueryStatement> queries = new ArrayList<>();
        queries.addAll(qg.retrieveQueries(template,"medium", query_count));
        return queries;
    }


    public static void main(String[] args) {
        Experiment e = new Experiment();
        PolicyGen pg = new PolicyGen();
//        List<Integer> users = pg.getAllUsers(true);
        List<QueryStatement> queries = e.getQueries(1, 50);
//        List <Integer> users = new ArrayList<>(Arrays.asList(26389, 15230, 30769, 12445, 36430, 21951,
//                13411, 7079, 364, 26000, 5949, 34372, 6371, 26083, 34290, 2917, 33425, 35503, 26927, 15007));
        List <Integer> users = new ArrayList<>(Arrays.asList(177));
        PolicyPersistor polper = new PolicyPersistor();
        String file_header = "Querier,Query_Cardinality,Query_Type,Number_Of_Policies, Query_Alone," +
                "Baseline_Policies,Number_of_Guards,Sieve \n";
        Writer writer = new Writer();
        writer.writeString(file_header, PolicyConstants.BE_POLICY_DIR, RESULTS_FILE);
        for (int i = 0; i < users.size(); i++) {
            String querier = String.valueOf(users.get(i));
            List<BEPolicy> allowPolicies = polper.retrievePolicies(querier,
                    PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
            if(allowPolicies == null) continue;
            System.out.println("Querier " + querier);
            for (int j = 0; j < queries.size(); j++) {
                writer.writeString(e.runBEPolicies(querier, queries.get(j).getQuery(), queries.get(j).getTemplate(), queries.get(j).getSelectivity(),
                        allowPolicies), PolicyConstants.BE_POLICY_DIR, RESULTS_FILE);
                QUERY_EXEC = false; //Baseline policy execution done only once
            }
        }
    }
}
