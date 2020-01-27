package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.db.MySQLResult;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.generation.policy.PolicyGen;
import edu.uci.ics.tippers.manager.GuardPersistor;
import edu.uci.ics.tippers.manager.PolicyPersistor;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.model.guard.GuardPart;
import edu.uci.ics.tippers.model.policy.BEPolicy;

import java.util.List;

public class GuardSelectivityExperiment {

    MySQLQueryManager mySQLQueryManager = new MySQLQueryManager();

    private float checkSelectivity(String query) {
        MySQLResult mySQLResult = mySQLQueryManager.runTimedQueryWithOutSorting(query, true);
        return (float) mySQLResult.getResultCount() / (float) PolicyConstants.NUMBER_OR_TUPLES;
    }

    public static void main(String[] args){
        GuardSelectivityExperiment gse = new GuardSelectivityExperiment();
        PolicyGen pg = new PolicyGen();
        List <Integer> users = pg.getAllUsers(true);;
        Writer writer = new Writer();
        writer.writeString("Querier, Number of Guards, Avg Selectivity, Avg Num of Policies \n", PolicyConstants.BE_POLICY_DIR, "expt2.csv");
        for (int i = 1; i <= users.size(); i++) {
            StringBuilder result = new StringBuilder();
            String querier = String.valueOf(users.get(i));
            PolicyPersistor polper = new PolicyPersistor();
            List<BEPolicy> allowPolicies = polper.retrievePolicies(querier,
                    PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
            GuardPersistor guardPersistor = new GuardPersistor();
            GuardExp guardExp = guardPersistor.retrieveGuardExpression(querier, "user", allowPolicies);
            if(allowPolicies == null) continue;
            double totalSel = 0.0;
            int numOfPolicies = 0;
            int numOfGuards = 0;
            for (int j = 0; j < guardExp.getGuardParts().size(); j++) {
                GuardPart gp = guardExp.getGuardParts().get(j);
                numOfGuards += 1;
                totalSel += gse.checkSelectivity(gp.getGuard().print());
                numOfPolicies += gp.getGuardPartition().getPolicies().size();
            }
            result.append(querier).append(",")
                    .append(numOfGuards).append(",")
                    .append(totalSel/numOfGuards).append(",")
                    .append(numOfPolicies/numOfGuards)
                    .append(" ").append("\n");
            System.out.println(result.toString());
            writer.writeString(result.toString(), PolicyConstants.BE_POLICY_DIR, "expt2.csv");
        }
    }
}
