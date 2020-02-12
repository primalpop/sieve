package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.db.MySQLResult;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.generation.policy.PolicyGen;
import edu.uci.ics.tippers.manager.PolicyPersistor;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.model.guard.GuardPart;
import edu.uci.ics.tippers.model.guard.SelectGuard;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class LargePolicyExperiment {

    static MySQLQueryManager mySQLQueryManager = new MySQLQueryManager();


    public static void main(String[] args) {
        PolicyGen pg = new PolicyGen();
        String RESULTS_FILE = "large_results.csv";
        List<Integer> users = pg.getAllUsers(true);
        PolicyPersistor polper = new PolicyPersistor();
        String file_header = "Number_Of_Policies,Baseline_Policies,Guard_Generation,Number_of_Guards,Total_Guard_Cardinality,Sieve\n";
        Writer writer = new Writer();
        writer.writeString(file_header, PolicyConstants.BE_POLICY_DIR, RESULTS_FILE);
        List<BEPolicy> bePolicies = new ArrayList<>();
        for (Integer user : users) {
            String querier = String.valueOf(user);
            bePolicies.addAll(polper.retrievePolicies(querier, PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW));
            if (bePolicies.size() > 2000) break;
        }
        int chunkSize = 100;
        AtomicInteger counter = new AtomicInteger();
        List<BEPolicy> chunkPolicies = new ArrayList<>();
        for (BEPolicy bePolicy: bePolicies) {
            if (counter.incrementAndGet() % chunkSize == 0) {
                StringBuilder rString = new StringBuilder();
                rString.append(chunkPolicies.size()).append(",");
                BEExpression beExpression = new BEExpression(chunkPolicies);
                //Baseline Policies
                String polEvalQuery = "With polEval as ( Select * from PRESENCE where " + beExpression.createQueryFromPolices() + "  )" ;
                MySQLResult tradResult = mySQLQueryManager.runTimedQueryExp(polEvalQuery + "SELECT * from polEval ", 1);
                Duration runTime = Duration.ofMillis(0);
                runTime = runTime.plus(tradResult.getTimeTaken());
                rString.append(runTime.toMillis()).append(",");
                System.out.println("Baseline inlining policies: No of Policies: " + beExpression.getPolicies().size() + " , Time: " + runTime.toMillis());
                //Guard Generation
                Duration guardGen = Duration.ofMillis(0);
                Instant fsStart = Instant.now();
                SelectGuard gh = new SelectGuard(beExpression, true);
                Instant fsEnd = Instant.now();
                guardGen = guardGen.plus(Duration.between(fsStart, fsEnd));
                rString.append(guardGen).append(",").append(gh.numberOfGuards()).append(",");
                System.out.println("Guard Generation time: " + guardGen + " Number of Guards: " + gh.numberOfGuards());
                //Computing Total Guard Cardinality
                GuardExp guardExp = gh.create();
                double guardTotalCard = 0.0;
                for (GuardPart gp: guardExp.getGuardParts()) {
                    guardTotalCard += mySQLQueryManager.checkSelectivity(gp.getGuard().print());
                }
                rString.append(guardTotalCard).append(",");
                //Sieve approach
                Duration execTime = Duration.ofMillis(0);
                String guard_hybrid_query = guardExp.inlineOrNot(true) + "Select * from polEval";
                MySQLResult execResult = mySQLQueryManager.runTimedQueryExp(guard_hybrid_query, 1);
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
