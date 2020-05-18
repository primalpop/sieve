package edu.uci.ics.tippers.execution.PaperExperiments;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.db.QueryResult;
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

/**
 * Experiments in MySQL
 */
public class Experiment {


    PolicyPersistor polper;
    QueryExplainer queryExplainer;
    MySQLQueryManager mySQLQueryManager;

    private static boolean QUERY_EXEC;
    private static boolean BASE_LINE_POLICIES;
    private static boolean BASELINE_UDF;
    private static boolean GUARD_POLICY_INLINE;
    private static boolean GUARD_UDF;
    private static boolean GUARD_INDEX;
    private static boolean QUERY_INDEX;
    private static boolean SIEVE_EXEC;
    private static boolean RESULT_CHECK;

    private long QUERY_EXECUTION_TIME;


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
                GUARD_POLICY_INLINE = Boolean.parseBoolean(props.getProperty("guard_policies"));
                BASELINE_UDF = Boolean.parseBoolean(props.getProperty("baseline_udf"));
                GUARD_UDF = Boolean.parseBoolean(props.getProperty("guard_udf"));
                GUARD_INDEX = Boolean.parseBoolean(props.getProperty("guard_index"));
                QUERY_INDEX = Boolean.parseBoolean(props.getProperty("query_index"));
                SIEVE_EXEC = Boolean.parseBoolean(props.getProperty("sieve_exec"));
                RESULT_CHECK = Boolean.parseBoolean(props.getProperty("resultCheck"));
                NUM_OF_REPS = Integer.parseInt(props.getProperty("num_repetitions"));
                RESULTS_FILE = props.getProperty("results_file");
            }
        } catch (IOException ie) {
            ie.printStackTrace();
        }
    }

    public String runBEPolicies(String querier, QueryStatement queryStatement, List<BEPolicy> bePolicies) {

        BEExpression beExpression = new BEExpression(bePolicies);
        StringBuilder resultString = new StringBuilder();
        resultString.append(querier).append(",")
                .append(userProfile(Integer.parseInt(querier))).append(",")
                .append(queryStatement.getTemplate()).append(",")
                .append(queryStatement.getSelectivity()).append(",")
                .append(bePolicies.size()).append(",");

        QueryExplainer qe = new QueryExplainer();
        double querySel = qe.estimateSelectivity(queryStatement);
        resultString.append(querySel).append(",");

        try {
            if (QUERY_EXEC){
                QueryResult queryResult;
                if(queryStatement.getTemplate() == 3)
                    queryResult = mySQLQueryManager.runTimedSubQuery(queryStatement.getQuery(), RESULT_CHECK);
                else
                    queryResult = mySQLQueryManager.runTimedQueryWithOutSorting(queryStatement.getQuery(), true);
                Duration runTime = Duration.ofMillis(0);
                runTime = runTime.plus(queryResult.getTimeTaken());
                System.out.println("Time taken by query alone " + runTime.toMillis());
                QUERY_EXECUTION_TIME = runTime.toMillis();
                resultString.append(QUERY_EXECUTION_TIME).append(",");
//                mySQLQueryManager.increaseTimeout(runTime.toMillis()); //Updating timeout to query exec time + constant

            }
            else resultString.append(QUERY_EXECUTION_TIME).append(",");

            if (BASE_LINE_POLICIES) {
                QueryResult tradResult = new QueryResult();
                String polEvalQuery = "With polEval as ( Select * from PRESENCE where "
                        + beExpression.createQueryFromPolices() + "  )" ;
                if(queryStatement.getTemplate() == 3)
                    tradResult = mySQLQueryManager.runTimedQueryExp(polEvalQuery + queryStatement.getQuery(), 1);
                else
                    tradResult = mySQLQueryManager.runTimedQueryExp(polEvalQuery + "SELECT * from polEval where "
                            + queryStatement.getQuery(), 1);
                Duration runTime = Duration.ofMillis(0);
                runTime = runTime.plus(tradResult.getTimeTaken());
                resultString.append(runTime.toMillis()).append(",");
                System.out.println("Baseline inlining policies: No of Policies: " + beExpression.getPolicies().size() + " , Time: " + runTime.toMillis());
            }

            if(BASELINE_UDF){
                QueryResult execResult;
                String udf_query = " and pcheck( " + querier + ", PRESENCE.user_id, PRESENCE.location_id, " +
                        "PRESENCE.start_date, PRESENCE.start_time, PRESENCE.user_profile, PRESENCE.user_group) = 1";
                if(queryStatement.getTemplate() == 3)
                    execResult = mySQLQueryManager.runTimedQueryExp(queryStatement.getQuery() + udf_query, 1);
                else
                    execResult = mySQLQueryManager.runTimedQueryExp("SELECT * from PRESENCE where "
                            + queryStatement.getQuery() + udf_query, 1);
                resultString.append(execResult.getTimeTaken().toMillis()).append(",");
                System.out.println("Baseline UDF: " + " , Time: " + execResult.getTimeTaken().toMillis());
            }

            GuardPersistor guardPersistor = new GuardPersistor();
            GuardExp guardExp = guardPersistor.retrieveGuardExpression(querier, "user", bePolicies);
            if(guardExp.getGuardParts().isEmpty()) return "empty";
            resultString.append(guardExp.getGuardParts().size()).append(",");
            double guardTotalCard = guardExp.getGuardParts().stream().mapToDouble(GuardPart::getCardinality).sum();
            resultString.append(guardTotalCard).append(",");


            if(GUARD_POLICY_INLINE) {
                //TODO: Does not work for template 3
                Duration execTime = Duration.ofMillis(0);
                String guard_query_with_union = guardExp.inlineRewrite(true);
                String guard_query_with_or = guardExp.inlineRewrite(false);
                guard_query_with_union += "Select * from polEval where " + queryStatement.getQuery();
                guard_query_with_or += "Select * from polEval where " + queryStatement.getQuery();
                QueryResult execResult = mySQLQueryManager.runTimedQueryExp(guard_query_with_union, NUM_OF_REPS);
                execTime = execTime.plus(execResult.getTimeTaken());
                resultString.append(execTime.toMillis()).append(",");
                System.out.println("Guard inline execution with union: "  + " Time: " + execTime.toMillis());
                execResult = mySQLQueryManager.runTimedQueryExp(guard_query_with_or, NUM_OF_REPS);
                execTime = execTime.plus(execResult.getTimeTaken());
                resultString.append(execTime.toMillis()).append(",");
                System.out.println("Guard inline execution with OR: "  + " Time: " + execTime.toMillis());
            }
            if(GUARD_UDF){
                //TODO: Does not work for template 3
                Duration execTime = Duration.ofMillis(0);
                String guard_query_with_union = guardExp.udfRewrite(true);
                String guard_query_with_or = guardExp.udfRewrite(false);
                guard_query_with_union += "Select * from polEval where " + queryStatement.getQuery();
                guard_query_with_or += "Select * from polEval where " + queryStatement.getQuery();
                QueryResult execResult = mySQLQueryManager.runTimedQueryExp(guard_query_with_union, NUM_OF_REPS);
                execTime = execTime.plus(execResult.getTimeTaken());
                resultString.append(execTime.toMillis()).append(",");
                System.out.println("Guard udf execution with union: "  + " Time: " + execTime.toMillis());
                execResult = mySQLQueryManager.runTimedQueryExp(guard_query_with_or, NUM_OF_REPS);
                execTime = execTime.plus(execResult.getTimeTaken());
                resultString.append(execTime.toMillis());
                System.out.println("Guard udf execution with OR: "  + " Time: " + execTime.toMillis());
            }
            if(GUARD_INDEX) {
                Duration execTime = Duration.ofMillis(0);
                String guard_hybrid_query = guardExp.inlineOrNot(true);
                if(queryStatement.getTemplate() == 3){
                    guard_hybrid_query +=  queryStatement.getQuery().replace("PRESENCE", "polEval");
                }
                else
                    guard_hybrid_query += "Select * from polEval where " + queryStatement.getQuery();
                QueryResult execResult = mySQLQueryManager.runTimedQueryExp(guard_hybrid_query, 1);
                execTime = execTime.plus(execResult.getTimeTaken());
                resultString.append(execTime.toMillis()).append(",");
                System.out.println("Guard Index execution : "  + " Time: " + execTime.toMillis());
            }
            if(QUERY_INDEX) {
                Duration execTime = Duration.ofMillis(0);
                String query_hint = qe.keyUsed(queryStatement);
                String queryPredicates = queryStatement.getQuery();
                String query_index_query;
                if(query_hint != null) {
                    if(queryStatement.getTemplate() == 3) {
                        queryPredicates = queryPredicates.replace("from PRESENCE", "from PRESENCE force index("
                                + query_hint +")" );
                        query_index_query = "SELECT * from ( " + queryPredicates + " ) as P where " + guardExp.createQueryWithOR();
                    }
                    else
                        query_index_query = "SELECT * from ( SELECT * from PRESENCE force index(" + query_hint
                                + ") where " + queryPredicates + " ) as P where " + guardExp.createQueryWithOR();
                    QueryResult execResult = mySQLQueryManager.runTimedQueryExp(query_index_query, 1);
                    execTime = execTime.plus(execResult.getTimeTaken());
                    resultString.append(execTime.toMillis()).append(",");
                    System.out.println("Query Index execution : "  + " Time: " + execTime.toMillis());
                }
                else resultString.append("NA").append(","); //No index scan used with query predicate
            }
            if(SIEVE_EXEC){
                Duration execTime = Duration.ofMillis(0);
                String guardQuery = guardExp.inlineOrNot(true);
                String query_hint = qe.keyUsed(queryStatement);
                String sieve_query = null;
                /** Calibration of choosing between IndexGuards and IndexQuery
                 *  based on the ratio of querySel/guardTotalCard. In template 3
                 *  because of the join, this ratio is a much smaller number.
                 */
                boolean indexGuards = querySel > 0.5 * guardTotalCard;
                if(queryStatement.getTemplate() == 3) {
                    indexGuards = querySel > 0.01 *guardTotalCard;
                }
                if (indexGuards || query_hint == null) { //Use Guards
                    if(queryStatement.getTemplate() == 3){
                        sieve_query = guardQuery + queryStatement.getQuery().replace("PRESENCE", "polEval");
                    }
                    else
                        sieve_query = guardQuery + "Select * from polEval where " + queryStatement.getQuery();
                    resultString.append("Guard Index").append(",");
                }
                else { //Use queries
                    if(queryStatement.getTemplate() == 3) {
                        String query_index = queryStatement.getQuery().replace("from PRESENCE", "from PRESENCE force index("
                                + query_hint +")" );
                        sieve_query = "SELECT * from ( " + query_index + " ) as P where " + guardExp.createQueryWithOR();
                    }
                    else
                        sieve_query = "SELECT * from ( SELECT * from PRESENCE force index(" + query_hint
                                + ") where " + queryStatement.getQuery() + " ) as P where " + guardExp.createQueryWithOR();
                    resultString.append("Query Index").append(",");
                }
                QueryResult execResult = mySQLQueryManager.runTimedQueryExp(sieve_query, NUM_OF_REPS);
                execTime = execTime.plus(execResult.getTimeTaken());
                resultString.append(execTime.toMillis());
                System.out.println("Sieve Query: "  + " Time: " + execTime.toMillis());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(resultString);
        return resultString.append("\n").toString();
    }

    public List<QueryStatement> getQueries(int template, int query_count){
        QueryGeneration qg = new QueryGeneration();
        List<QueryStatement> queries = new ArrayList<>();
        queries.addAll(qg.retrieveQueries(template,"all", query_count));
        return queries;
    }

    private String userProfile(int querier){
        List <Integer> faculty = new ArrayList<>(Arrays.asList(1023, 5352, 11043, 13353, 18575));
        List <Integer> undergrad = new ArrayList<>(Arrays.asList(4686, 7632, 12555, 15936, 15007));
        List<Integer> grad = new ArrayList<>(Arrays.asList(100, 532, 5990, 11815, 32467));
        List<Integer> staff = new ArrayList<>(Arrays.asList(888, 2550, 5293, 9733, 20021));
        if(faculty.contains(querier)) return "faculty";
        else if(undergrad.contains(querier)) return "undergrad";
        else if(grad.contains(querier)) return "graduate";
        else return "staff" ;
    }

    public static void main(String[] args) {
        Experiment e = new Experiment();
        PolicyGen pg = new PolicyGen();
//        List<Integer> users = pg.getAllUsers(true);
        //users with increasing number of guards
//        List <Integer> users = new ArrayList<>(Arrays.asList(26389, 15230, 30769, 12445, 36430, 21951,
//                13411, 7079, 364, 26000, 5949, 34372, 6371, 26083, 34290, 2917, 33425, 35503, 26927, 15007));
        //users with guards of increasing cardinality
//        List <Integer> users = new ArrayList<>(Arrays.asList(14215, 56, 2050, 2819, 37, 625, 23519, 8817, 6215, 387,
//                945, 8962, 23416, 34035));
        //users with increasing number of policies
        List <Integer> faculty = new ArrayList<>(Arrays.asList(1023, 5352, 11043, 13353, 18575));
        List <Integer> undergrad = new ArrayList<>(Arrays.asList(4686, 7632, 12555, 15936, 15007));
        List<Integer> grad = new ArrayList<>(Arrays.asList(100, 532, 5990, 11815, 32467));
        List<Integer> staff = new ArrayList<>(Arrays.asList(888, 2550, 5293, 9733, 20021));
        List<Integer> users = new ArrayList<>();
        users.addAll(faculty);
        users.addAll(undergrad);
        users.addAll(grad);
        users.addAll(staff);
        PolicyPersistor polper = new PolicyPersistor();
        String file_header = "Querier,Querier_Profile,Query_Type,Query_Cardinality,Number_Of_Policies,Estimated_QPS,Query_Alone," +
                "Baseline_Policies, Baseline_UDF,Number_of_Guards,Total_Guard_Cardinality,With_Guard_Index,With_Query_Index,Sieve_Parameters, Sieve\n";
        Writer writer = new Writer();
        writer.writeString(file_header, PolicyConstants.BE_POLICY_DIR, RESULTS_FILE);
        List<QueryStatement> queries = e.getQueries(2, 10);
        queries.addAll(e.getQueries(3, 10));
        for (int j = 0; j < queries.size(); j++) {
            System.out.println("Total Query Selectivity " + queries.get(j).getSelectivity());
            for (int i = 0; i < users.size(); i++) {
                String querier = String.valueOf(users.get(i));
                List<BEPolicy> allowPolicies = polper.retrievePolicies(querier,
                        PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
                if (allowPolicies == null) continue;
                System.out.println("Querier " + querier);
                writer.writeString(e.runBEPolicies(querier, queries.get(j),
                        allowPolicies), PolicyConstants.BE_POLICY_DIR, RESULTS_FILE);
                QUERY_EXEC = false;
            }
            QUERY_EXEC = true;
        }
    }
}
