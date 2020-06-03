package edu.uci.ics.tippers.execution.ReviewResponse;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.QueryManager;
import edu.uci.ics.tippers.db.QueryResult;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.generation.policy.WiFiDataSet.PolicyUtil;
import edu.uci.ics.tippers.manager.PolicyPersistor;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.model.guard.SelectGuard;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

//TODO: Delete this and maintain only test class
public class MysqlTest {

    private static final int MAX_POLICY_NUMBER = 2000;

    public static void main(String[] args) {
        PolicyConstants.initialize();
        QueryManager queryManager = new QueryManager();
        PolicyUtil pg = new PolicyUtil();
        String RESULTS_FILE = "mysql_large_results.csv";
        List<Integer> users = pg.getAllUsers(true);
        PolicyPersistor polper = PolicyPersistor.getInstance();
//        String file_header = "Number_Of_Policies,Baseline_Policies,Guard_Generation,Number_of_Guards,Total_Guard_Cardinality,Sieve\n";
        String file_header = "Number_Of_Policies,Baseline_Policies,Guard_Generation,Number_of_Guards,Sieve\n";
        Writer writer = new Writer();
        writer.writeString(file_header, PolicyConstants.BE_POLICY_DIR, RESULTS_FILE);
        List<BEPolicy> bePolicies = new ArrayList<>();
        Random rand = new Random();
        while(bePolicies.size() < MAX_POLICY_NUMBER) {
            String querier = String.valueOf(users.get(rand.nextInt(users.size())));
            bePolicies.addAll(polper.retrievePolicies(querier, PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW));
        }
        int chunkSize = 100;
        int counter = 0;
        List<BEPolicy> chunkPolicies = new ArrayList<>();
        boolean QUERY_EXEC_TIMEOUT = false;
        for (BEPolicy bePolicy: bePolicies) {
            if (++counter % chunkSize == 0) {
                StringBuilder rString = new StringBuilder();
                rString.append(chunkPolicies.size()).append(",");
                BEExpression beExpression = new BEExpression(chunkPolicies);
                //Baseline Policies
                if(!QUERY_EXEC_TIMEOUT) {
                    String polEvalQuery = "With polEval as ( Select * from PRESENCE where " + beExpression.createQueryFromPolices() + "  )" ;
                    QueryResult tradResult = queryManager.runTimedQueryExp(polEvalQuery + "SELECT * from polEval ", 1);
                    Duration runTime = Duration.ofMillis(0);
                    runTime = runTime.plus(tradResult.getTimeTaken());
                    if(runTime.equals(PolicyConstants.MAX_DURATION)) QUERY_EXEC_TIMEOUT = true;
                    rString.append(runTime.toMillis()).append(",");
                    System.out.println("Baseline inlining policies: No of Policies: " + beExpression.getPolicies().size() + " , Time: " + runTime.toMillis());
                }
                else rString.append(PolicyConstants.MAX_DURATION.toMillis()).append(",");

                //Guard Generation
                Duration guardGen = Duration.ofMillis(0);
                Instant fsStart = Instant.now();
                SelectGuard gh = new SelectGuard(beExpression, true);
                Instant fsEnd = Instant.now();
                guardGen = guardGen.plus(Duration.between(fsStart, fsEnd));
                rString.append(guardGen.toMillis()).append(",").append(gh.numberOfGuards()).append(",");
                System.out.println("Guard Generation time: " + guardGen + " Number of Guards: " + gh.numberOfGuards());
                //Computing Total Guard Cardinality
                GuardExp guardExp = gh.create();
                double guardTotalCard = 0.0;
//                for (GuardPart gp: guardExp.getGuardParts()) {
//                    guardTotalCard += queryManager.checkSelectivity(gp.getGuard().print());
//                }
//                rString.append(guardTotalCard).append(",");
                //Sieve approach
                Duration execTime = Duration.ofMillis(0);
                String guard_hybrid_query = guardExp.queryRewrite(true,true);
                QueryResult execResult = queryManager.runTimedQueryExp(guard_hybrid_query, 1);
                execTime = execTime.plus(execResult.getTimeTaken());
                rString.append(execTime.toMillis()).append("\n");
                System.out.println("Guard Query execution : "  + " Time: " + execTime.toMillis());
                //Print the csv line
                writer.writeString(rString.toString(), PolicyConstants.BE_POLICY_DIR, RESULTS_FILE);
            }
            else chunkPolicies.add(bePolicy);
        }
    }
}
