package edu.uci.ics.tippers.execution.PaperExperiments;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.generation.policy.WiFiDataSet.PolicyUtil;
import edu.uci.ics.tippers.persistor.GuardPersistor;
import edu.uci.ics.tippers.persistor.PolicyPersistor;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.model.guard.GuardPart;
import edu.uci.ics.tippers.model.policy.BEPolicy;

import java.util.List;

/**
 * Experiment 1.2 in the paper except for Guard effectiveness
 * TODO: Include guard effectiveness as part of this experiment
 */
public class GuardSelectivityExperiment {

    public static void main(String[] args){
        String fileName = "helloGuard.csv";
        PolicyUtil pg = new PolicyUtil();
        List <Integer> users = pg.getAllUsers(true);;
        Writer writer = new Writer();
        writer.writeString("Querier, Number of Policies, Number of Guards, Guard cardinality \n", PolicyConstants.EXP_RESULTS_DIR, fileName);
        for (int i = 1; i <= users.size(); i++) {
            StringBuilder result = new StringBuilder();
            String querier = String.valueOf(users.get(i));
            PolicyPersistor polper = PolicyPersistor.getInstance();
            List<BEPolicy> allowPolicies = polper.retrievePolicies(querier,
                    PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
            GuardPersistor guardPersistor = new GuardPersistor();
            GuardExp guardExp = guardPersistor.retrieveGuardExpression(querier, "user", allowPolicies);
            if(allowPolicies == null) continue;
            double totalGuardCard = 0.0;
            for (int j = 0; j < guardExp.getGuardParts().size(); j++) {
                GuardPart gp = guardExp.getGuardParts().get(j);
                totalGuardCard += gp.getCardinality();
            }
            result.append(querier).append(",")
                    .append(allowPolicies.size()).append(",")
                    .append(guardExp.getGuardParts().size()).append(",")
                    .append(totalGuardCard)
                    .append("\n");
            System.out.println(result.toString());
            writer.writeString(result.toString(), PolicyConstants.EXP_RESULTS_DIR, fileName);
        }
    }
}
