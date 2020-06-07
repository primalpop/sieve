package edu.uci.ics.tippers.execution.ReviewResponse;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.QueryManager;
import edu.uci.ics.tippers.db.QueryResult;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.generation.policy.WiFiDataSet.PolicyUtil;
import edu.uci.ics.tippers.manager.PolicyPersistor;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.model.guard.GuardPart;
import edu.uci.ics.tippers.model.guard.SelectGuard;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;

import java.time.Duration;
import java.util.*;

public class DBMSTest {

    private static final int MAX_POLICY_NUMBER = 10;
    private static final int CHUNK_SIZE = 5;
    private static final int EXP_REPETITIONS = 1;

    public static void main(String[] args) {
        PolicyConstants.initialize();
        System.out.println("Running on " + PolicyConstants.DBMS_CHOICE + " at " + PolicyConstants.DBMS_LOCATION + " with "
                +  PolicyConstants.TABLE_NAME + " and " + PolicyConstants.getNumberOfTuples() + " tuples");
        QueryManager queryManager = new QueryManager();
        PolicyUtil pg = new PolicyUtil();
        String RESULTS_FILE = PolicyConstants.DBMS_CHOICE + "_results.csv";
        List<Integer> users = pg.getAllUsers(true);
        Collections.sort(users);
        PolicyPersistor polper = PolicyPersistor.getInstance();
        String file_header = "Number_Of_Policies,Result Count,Baseline_Policies,Number_of_Guards,Total_Guard_Cardinality,Sieve(with Union),Sieve(with OR)\n";
        Writer writer = new Writer();
        writer.writeString(file_header, PolicyConstants.BE_POLICY_DIR, RESULTS_FILE);
        List<BEPolicy> bePolicies = new ArrayList<>();
        for (Integer user : users) {
            String querier = String.valueOf(user);
            if (bePolicies.size() > MAX_POLICY_NUMBER) break;
            bePolicies.addAll(polper.retrievePolicies(querier, PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW));
        }
        bePolicies = bePolicies.subList(0, MAX_POLICY_NUMBER);
        List<BEPolicy> chunkPolicies = new ArrayList<>();
        boolean QUERY_EXEC_TIMEOUT = false;
        for (int i = 0; i < bePolicies.size(); i++) {
            BEPolicy bePolicy = bePolicies.get(i);
            if (i % CHUNK_SIZE == 0 && i != 0) {
                StringBuilder rString = new StringBuilder();
                rString.append(chunkPolicies.size()).append(",");
                BEExpression beExpression = new BEExpression(chunkPolicies);
                //Baseline Policies
//                if(!QUERY_EXEC_TIMEOUT) {
//                    String polEvalQuery = "With polEval as ( Select * from PRESENCE where " + beExpression.createQueryFromPolices() + "  )" ;
//                    QueryResult tradResult = queryManager.runTimedQueryExp(polEvalQuery + "SELECT * from polEval ", EXP_REPETITIONS);
//                    System.out.println("Vanilla rewrite Result count: " + tradResult.getResultCount());
//                    Duration runTime = Duration.ofMillis(0);
//                    runTime = runTime.plus(tradResult.getTimeTaken());
//                    if(runTime.equals(PolicyConstants.MAX_DURATION)) QUERY_EXEC_TIMEOUT = true;
//                    rString.append(tradResult.getResultCount()).append(",");
//                    rString.append(runTime.toMillis()).append(",");
//                    System.out.println("Baseline inlining policies: No of Policies: " + beExpression.getPolicies().size() + " , Time: " + runTime.toMillis());
//                }
//                else {
//                    rString.append(PolicyConstants.INFINTIY).append(",");
//                    rString.append(PolicyConstants.MAX_DURATION.toMillis()).append(",");
//                }

                //Guard Generation
                SelectGuard gh = new SelectGuard(beExpression, true);
                System.out.println("Number of policies: " + beExpression.getPolicies().size() + " Number of Guards: " + gh.numberOfGuards());
                rString.append(gh.numberOfGuards()).append(",");
                //Computing Total Guard Cardinality
                GuardExp guardExp = gh.create();
                double guardTotalCard = 0.0;
                for (GuardPart gp: guardExp.getGuardParts()) {
                    guardTotalCard += queryManager.checkSelectivity(gp.getGuard().print());
                }
                rString.append(guardTotalCard).append(",");
                //Guard only selectivity
                QueryResult guardOnlyQuery = queryManager.runTimedQueryExp(guardExp.createGuardOnlyQuery(), 1);
                System.out.println(guardOnlyQuery.getTimeTaken());
                rString.append(guardOnlyQuery.getTimeTaken()).append("\n");
                //Sieve approach
                String guard_query_union = guardExp.queryRewrite(true, true);
//                QueryResult execResultUnion = queryManager.runTimedQueryExp(guard_query_union, EXP_REPETITIONS);
//                String guard_query_or = guardExp.queryRewrite(true, false);
//                QueryResult execResultOr = queryManager.runTimedQueryExp(guard_query_or, EXP_REPETITIONS);
//                rString.append(execResultUnion.getTimeTaken().toMillis()).append(",");
//                rString.append(execResultOr.getTimeTaken().toMillis()).append("\n");
//                System.out.println("Guard Query execution (with Union): "  + " Time: " + execResultUnion.getTimeTaken().toMillis());
//                System.out.println("Guard Query result count (with Union): "  + " Time: " + execResultUnion.getResultCount());
//                System.out.println("Guard Query execution (with OR): "  + execResultOr.getTimeTaken().toMillis());
//                System.out.println("Guard Query result count (with OR): "  + " Time: " + execResultUnion.getResultCount());
                //Print the csv line
                writer.writeString(rString.toString(), PolicyConstants.BE_POLICY_DIR, RESULTS_FILE);
            }
            else chunkPolicies.add(bePolicy);
        }
    }
}
