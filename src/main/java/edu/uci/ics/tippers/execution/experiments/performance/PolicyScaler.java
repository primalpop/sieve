package edu.uci.ics.tippers.execution.experiments.performance;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.dbms.QueryManager;
import edu.uci.ics.tippers.dbms.QueryResult;
import edu.uci.ics.tippers.execution.ExpResult;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.persistor.PolicyPersistor;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.model.guard.SelectGuard;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;

import java.util.*;


/**
 * Experiments on WiFi Dataset and Mall Dataset for testing baselines against Sieve on MySQL and PostgreSQL
 * Experiments 4 and 5 in the paper
 */

/**
 * Number of policies: Users
 * < 100 : [10586, 32558, 21587, 13136, 13003, 34376, 10983]
 * 100 < x < 125 : [12096, 23957, 32804, 34755, 16371, 18112]
 * 125 < x < 150 : [20351, 23851, 36011, 11827, 14704, 22315, 15539, 28684]
 * 150 < x < 175 : [16436, 7683, 31432, 16880, 9001, 24492, 13540 ]
 * 175 < x < 200 : [10504, 26968, 22018, 18646, 18029, 29559, 6508, 17287]
 * 200 < x < 225 : [15135, 3832, 33791, 2469, 25582, 20568, 29807, 33918]
 * 225 < x < 250 : [25145, 32738, 19596, 5936, 6916, 16749, 13855, 16718, 28372]
 * 250 < x < 275 : [16915, 4817, 12054, 4355, 30276, 6815 ]
 * 275 < x < 300 : [22713, 14886, 26073, 13353, 16620, 14931, 15039]
 * 300 < x < 350 : [22636, 22995, 32467, 15007, 18575, 23422, 26801, 11815, 18094, 2039]
 */

public class PolicyScaler {

    private static final int EXP_REPETITIONS = 5;
    private static final int RANDOM_SAMPLING =  5;

    private static QueryManager queryManager;

    private static long baseline(BEExpression beExpression) {
        long baseline = 0;
        if(PolicyConstants.DBMS_CHOICE.equalsIgnoreCase(PolicyConstants.MYSQL_DBMS)) {
            String polIndexQuery = beExpression.createIndexQuery();
            QueryResult indResult = queryManager.runTimedQueryExp(polIndexQuery, EXP_REPETITIONS);
            System.out.println("Index rewrite Result count: " + indResult.getResultCount());
            baseline += indResult.getTimeTaken().toMillis();
        }
        else {
            String polEvalQuery = "With polEval as ( Select * from " + PolicyConstants.TABLE_NAME + " where " + beExpression.createQueryFromPolices() + "  )" ;
            QueryResult tradResult = queryManager.runTimedQueryExp(polEvalQuery + "SELECT * from polEval ", EXP_REPETITIONS);
            System.out.println("Vanilla rewrite Result count: " + tradResult.getResultCount());
            baseline += tradResult.getTimeTaken().toMillis();
        }
        return baseline;
    }

    public void runExperiment(){
        PolicyConstants.initialize();
        queryManager = new QueryManager();
        System.out.println("Running on " + PolicyConstants.DBMS_CHOICE + " at " + PolicyConstants.DBMS_LOCATION + " with "
                +  PolicyConstants.TABLE_NAME.toLowerCase() + " and " + PolicyConstants.getNumberOfTuples() + " tuples");
        Map<Integer, List<ExpResult>> resultMap = new TreeMap<>();
        String RESULTS_FILE = PolicyConstants.TABLE_NAME.toLowerCase() + "_" + PolicyConstants.DBMS_CHOICE + "_results.csv";
        String file_header = "Number Of Policies,Number of Guards,Baseline,Sieve\n";
        Writer writer = new Writer();
        writer.writeString(file_header, PolicyConstants.EXP_RESULTS_DIR, RESULTS_FILE);

        List<Integer> users = new ArrayList<>();
        List<Integer> xpoints = new ArrayList<>();
        if(PolicyConstants.TABLE_NAME.equalsIgnoreCase(PolicyConstants.WIFI_TABLE)){
            users = new ArrayList<>(Arrays.asList(22636, 22995, 32467, 15007, 18575, 23422, 26801, 11815, 18094, 2039));
            xpoints = new ArrayList<>(Arrays.asList(75, 100, 125, 150, 175, 200, 225, 250, 275, 300));
        }
        else  if(PolicyConstants.TABLE_NAME.equalsIgnoreCase(PolicyConstants.MALL_TABLE)) {
            users = new ArrayList<>(Arrays.asList(8, 7, 29, 23, 28));
            xpoints = new ArrayList<>(Arrays.asList(100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200));
        }

        PolicyPersistor polper = PolicyPersistor.getInstance();
        for (Integer user : users) {
            String querier = String.valueOf(user);
            List<BEPolicy> bePolicies = polper.retrievePolicies(querier, PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
            for (int k = 0; k < RANDOM_SAMPLING; k++) {
                Collections.shuffle(bePolicies);
                for (Integer xpoint : xpoints) {
                    List<BEPolicy> subPolicies = bePolicies.subList(0, xpoint);
                    ExpResult expResult = new ExpResult();
                    expResult.setNumberOfPolicies(subPolicies.size());
                    BEExpression beExpression = new BEExpression(subPolicies);
                    expResult.setBaselineR(baseline(beExpression));
                    //Guard Generation
                    SelectGuard gh = new SelectGuard(beExpression, true);
                    System.out.println("Number of policies: " + beExpression.getPolicies().size() + " Number of Guards: " + gh.numberOfGuards());
                    expResult.setNumberOfGuards(gh.numberOfGuards());
                    GuardExp guardExp = gh.create();
                    String guard_query_union = guardExp.queryRewrite(true, true);
                    QueryResult execResultUnion = queryManager.runTimedQueryExp(guard_query_union, EXP_REPETITIONS);
                    expResult.setSieveR(execResultUnion.getTimeTaken().toMillis());
                    if (!resultMap.containsKey(xpoint)) {
                        List<ExpResult> expResults = new ArrayList<>();
                        expResults.add(expResult);
                        resultMap.put(xpoint, expResults);
                    } else resultMap.get(xpoint).add(expResult);
                }
            }
        }
        for (int x: resultMap.keySet()) {
            StringBuilder rString = new StringBuilder();
            OptionalDouble baselineR = resultMap.get(x).stream().mapToLong(ExpResult::getBaselineR).average();
            OptionalDouble sieveR = resultMap.get(x).stream().mapToLong(ExpResult::getSieveR).average();
            OptionalDouble numOfGuards = resultMap.get(x).stream().mapToInt(ExpResult::getNumberOfGuards).average();
            rString.append(x).append(",")
                    .append(Math.round(numOfGuards.getAsDouble())).append(",")
                    .append(String.format("%.2f", baselineR.getAsDouble())).append(",")
                    .append(String.format("%.2f", sieveR.getAsDouble())).append("\n");
            writer.writeString(rString.toString(), PolicyConstants.EXP_RESULTS_DIR, RESULTS_FILE);
        }
    }
}
