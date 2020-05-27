package edu.uci.ics.tippers.execution.PaperExperiments;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.QueryManager;
import edu.uci.ics.tippers.db.QueryResult;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.generation.query.QueryExplainer;
import edu.uci.ics.tippers.manager.GuardPersistor;
import edu.uci.ics.tippers.manager.PolicyPersistor;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.model.guard.GuardPart;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Experiment 3.2 in the paper
 */
public class ExperimentGuards {


    PolicyPersistor polper;
    QueryExplainer queryExplainer;
    QueryManager queryManager;

    private static String RESULTS_FILE;

    public ExperimentGuards() {
        polper = new PolicyPersistor();
        queryExplainer = new QueryExplainer();
        queryManager = new QueryManager();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        RESULTS_FILE = "expts_3.2_results.csv";
    }

    public String runBEPolicies(String querier, List<BEPolicy> bePolicies) {

        BEExpression beExpression = new BEExpression(bePolicies);
        StringBuilder resultString = new StringBuilder();
        resultString.append(querier).append(",")
                .append(bePolicies.size()).append(",");

        try {
            GuardPersistor guardPersistor = new GuardPersistor();
            GuardExp guardExp = guardPersistor.retrieveGuardExpression(querier, "user", bePolicies);
            if (guardExp.getGuardParts().isEmpty()) return "empty";
            resultString.append(guardExp.getGuardParts().size()).append(",");
            double guardTotalCard = guardExp.getGuardParts().stream().mapToDouble(GuardPart::getCardinality).sum();
            resultString.append(guardTotalCard).append(",");


            Duration execTime = Duration.ofMillis(0);
            String guardQuery = guardExp.inlineOrNot(true);
            String sieve_query = guardQuery + "Select * from polEval ";
            resultString.append("Guard Index").append(",");


            QueryResult execResult = queryManager.runTimedQueryExp(sieve_query, 3);
            execTime = execTime.plus(execResult.getTimeTaken());
            resultString.append(execTime.toMillis());
            System.out.println("Sieve Query: " + " Time: " + execTime.toMillis());

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(resultString);
        return resultString.append("\n").toString();
    }

    public static void main(String[] args) {
        ExperimentGuards eg = new ExperimentGuards();
        //users with different guard numbers and guard cardinalities
        List<Integer> ll = new ArrayList<>(Arrays.asList(22995, 18575, 18094, 2039));
        List<Integer> hl = new ArrayList<>(Arrays.asList(22636, 15007, 26801));
        List<Integer> lh = new ArrayList<>(Arrays.asList(11815));
        List<Integer> hh = new ArrayList<>(Arrays.asList(32647, 20021));
        List<Integer> users = new ArrayList<>(Arrays.asList(32467));
//        users.addAll(ll);
//        users.addAll(hl);
//        users.addAll(lh);
//        users.addAll(hh);
        PolicyPersistor polper = new PolicyPersistor();
        String file_header = "Querier,Number_Of_Policies,Number_of_Guards,Total_Guard_Cardinality, Sieve\n";
        Writer writer = new Writer();
        writer.writeString(file_header, PolicyConstants.BE_POLICY_DIR, RESULTS_FILE);
        for (int i = 0; i < users.size(); i++) {
            String querier = String.valueOf(users.get(i));
            List<BEPolicy> allowPolicies = polper.retrievePolicies(querier,
                    PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
            if (allowPolicies == null) continue;
            System.out.println("Querier " + querier);
            writer.writeString(eg.runBEPolicies(querier, allowPolicies), PolicyConstants.BE_POLICY_DIR, RESULTS_FILE);
        }
    }
}
