package edu.uci.ics.tippers.execution.ReviewResponse;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.QueryManager;
import edu.uci.ics.tippers.db.QueryResult;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.manager.PolicyPersistor;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.model.guard.SelectGuard;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;

import java.util.*;

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

public class RealPolicyScale {


    private static final int EXP_REPETITIONS = 1;

    public static void main(String[] args){

        List<Integer> u1 = new ArrayList<>(Arrays.asList(10586, 32558, 21587, 13136, 13003, 34376, 10983));
        List <Integer> u2 = new ArrayList<>(Arrays.asList(12096, 23957, 32804, 34755, 16371, 18112));
        List<Integer> u3 = new ArrayList<>(Arrays.asList(20351, 23851, 36011, 11827, 14704, 22315));
        List<Integer> u4 = new ArrayList<>(Arrays.asList(16436, 7683, 31432, 16880, 9001, 24492));
        List<Integer> u5 = new ArrayList<>(Arrays.asList(10504, 26968, 22018, 18646, 18029, 29559));
        List<Integer> u6 = new ArrayList<>(Arrays.asList(15135, 3832, 33791, 2469, 25582, 20568));
        List<Integer> u7 = new ArrayList<>(Arrays.asList(25145, 32738, 19596, 5936, 6916, 16749));
        List<Integer> u8 = new ArrayList<>(Arrays.asList(16915, 4817, 12054, 4355, 30276, 6815));
        List<Integer> u9 = new ArrayList<>(Arrays.asList(22713, 14886, 26073, 13353, 16620, 14931));
        List<Integer> u10 = new ArrayList<>(Arrays.asList(22636, 22995, 15007, 18575, 23422, 26801));

        Map<Integer, List<Integer>> userMap = new TreeMap<>();
        userMap.put(75, u1);
        userMap.put(110, u2);
        userMap.put(135, u3);
        userMap.put(160, u4);
        userMap.put(185, u5);
        userMap.put(210, u6);
        userMap.put(235, u7);
        userMap.put(260, u8);
        userMap.put(285, u9);
        userMap.put(310, u10);

        PolicyConstants.initialize();
        System.out.println("Running on " + PolicyConstants.DBMS_CHOICE + " at " + PolicyConstants.DBMS_LOCATION + " with "
                +  PolicyConstants.TABLE_NAME + " and " + PolicyConstants.getNumberOfTuples() + " tuples");
        QueryManager queryManager = new QueryManager();
        String RESULTS_FILE = PolicyConstants.DBMS_CHOICE + "_results.csv";
        PolicyPersistor polper = PolicyPersistor.getInstance();
        String file_header = "Number Of Policies,Number of Guards,BaselineP,";
        if(PolicyConstants.DBMS_CHOICE.equalsIgnoreCase(PolicyConstants.MYSQL_DBMS))
            file_header +="BaselineIndex,";
        file_header += "Sieve(with Union)\n";
        Writer writer = new Writer();
        writer.writeString(file_header, PolicyConstants.BE_POLICY_DIR, RESULTS_FILE);
        for (int u:userMap.keySet()) {
            long baselineP = 0, baselineI = 0, sieve = 0;
            int numberOfGuards = 0;
            List<Integer> segUsers = userMap.get(u);
            StringBuilder rString = new StringBuilder();
            for (int i = 0; i <  segUsers.size(); i++) {
                String querier = String.valueOf(segUsers.get(i));
                List<BEPolicy> bePolicies = polper.retrievePolicies(querier, PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
                BEExpression beExpression = new BEExpression(bePolicies);
                //Baseline Policies
                String polEvalQuery = "With polEval as ( Select * from PRESENCE where " + beExpression.createQueryFromPolices() + "  )" ;
                QueryResult tradResult = queryManager.runTimedQueryExp(polEvalQuery + "SELECT * from polEval ", EXP_REPETITIONS);
                System.out.println("Vanilla rewrite Result count: " + tradResult.getResultCount());
                baselineP += tradResult.getTimeTaken().toMillis();
                //Baseline Index Policies only for MySQL
                if(PolicyConstants.DBMS_CHOICE.equalsIgnoreCase(PolicyConstants.MYSQL_DBMS)) {
                    String polIndexQuery = beExpression.createIndexQuery();
                    QueryResult indResult = queryManager.runTimedQueryExp(polIndexQuery, EXP_REPETITIONS);
                    System.out.println("Index rewrite Result count: " + indResult.getResultCount());
                    baselineI += indResult.getTimeTaken().toMillis();
                }
                //Guard Generation
                SelectGuard gh = new SelectGuard(beExpression, true);
                System.out.println("Number of policies: " + beExpression.getPolicies().size() + " Number of Guards: " + gh.numberOfGuards());
                numberOfGuards += gh.numberOfGuards();
                GuardExp guardExp = gh.create();
                String guard_query_union = guardExp.queryRewrite(true, true);
                QueryResult execResultUnion = queryManager.runTimedQueryExp(guard_query_union, EXP_REPETITIONS);
                sieve += execResultUnion.getTimeTaken().toMillis();
                System.out.println("Guard Query execution (with Union): "  + " Time: " + execResultUnion.getTimeTaken().toMillis());
                System.out.println("Guard Query result count (with Union): "  + " Time: " + execResultUnion.getResultCount());
            }
            rString.append(u).append(",");
            rString.append(numberOfGuards/segUsers.size()).append(",");
            rString.append(baselineP/segUsers.size()).append(",");
            if(PolicyConstants.DBMS_CHOICE.equalsIgnoreCase(PolicyConstants.MYSQL_DBMS))
                rString.append(baselineI/segUsers.size()).append(",");
            rString.append(sieve/segUsers.size()).append("\n");
            writer.writeString(rString.toString(), PolicyConstants.BE_POLICY_DIR, RESULTS_FILE);
        }
    }

}
