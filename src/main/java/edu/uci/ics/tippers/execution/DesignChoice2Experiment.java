package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.db.MySQLResult;
import edu.uci.ics.tippers.generation.query.QueryExplainer;
import edu.uci.ics.tippers.manager.GuardPersistor;
import edu.uci.ics.tippers.manager.PolicyPersistor;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.model.guard.GuardPart;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.query.QueryStatement;

import java.util.List;

public class DesignChoice2Experiment {

    MySQLQueryManager mySQLQueryManager = new MySQLQueryManager();

    private float checkSelectivity(String query) {
        MySQLResult mySQLResult = mySQLQueryManager.runTimedQueryWithOutSorting(query, true);
        return (float) mySQLResult.getResultCount() / (float) PolicyConstants.NUMBER_OR_TUPLES;
    }


    private void runExpt(){
        Experiment e = new Experiment();
        QueryExplainer qe = new QueryExplainer();
        List<QueryStatement> queries = e.getQueries();
        PolicyPersistor polper = new PolicyPersistor();

        String querier = String.valueOf(23868);
        List<BEPolicy> allowPolicies = polper.retrievePolicies(querier,
                PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
        GuardPersistor guardPersistor = new GuardPersistor();
        GuardExp guardExp = guardPersistor.retrieveGuardExpression(querier, "user", allowPolicies);
        System.out.println("Querier: " + querier + ", # Policies: " + allowPolicies.size() + ", # guards: " + guardExp.getGuardParts().size());
        GuardSelectivityExperiment gse = new GuardSelectivityExperiment();
        double totalSel = 0.0;
        for (int j = 0; j < guardExp.getGuardParts().size(); j++) {
            GuardPart gp = guardExp.getGuardParts().get(j);
            totalSel += checkSelectivity(gp.getGuard().print());
        }
        System.out.println("Guard Selectivity,Query Selectivity,Without Hint,With Guard Hint Inline,With Query Hint");
        for (int j = 0; j < queries.size(); j=j+5) {
            StringBuilder rString = new StringBuilder();
            rString.append(totalSel).append(",");
            rString.append(queries.get(j).getSelectivity()).append(",");
            String queryPredicates = queries.get(j).getQuery();
            BEExpression beExpression = new BEExpression(allowPolicies);
            String baselineQuery = "With polEval as ( Select * from PRESENCE where "
                    + beExpression.createQueryFromPolices() + "  )"
                    + "Select * from polEval";
            String guard_query_with_hint_inline = guardExp.inlineRewrite(true);
            guard_query_with_hint_inline += "Select * from polEval where " + queryPredicates;
            String guard_query_with_hint_udf = guardExp.udfRewrite(true);
            guard_query_with_hint_udf += "Select * from polEval where " + queryPredicates;
            String guard_query_with_choice = guardExp.inlineOrNot(true);
            guard_query_with_choice += "Select * from polEval where " + queryPredicates;
            String guard_query_without_hint = guardExp.rewriteWithoutHint();
            guard_query_without_hint += "Select * from polEval where " + queryPredicates;
            MySQLResult execResult = mySQLQueryManager.runTimedQueryExp(baselineQuery, 1);
            rString.append(execResult.getTimeTaken().toMillis()).append(",");
            execResult = mySQLQueryManager.runTimedQueryExp(guard_query_without_hint, 1);
            rString.append(execResult.getTimeTaken().toMillis()).append(",");
            execResult = mySQLQueryManager.runTimedQueryExp(guard_query_with_hint_inline, 1);
            rString.append(execResult.getTimeTaken().toMillis()).append(",");
//            execResult =  mySQLQueryManager.runTimedQueryExp(guard_query_with_hint_udf, 1);
//            rString.append(execResult.getTimeTaken().toMillis()).append(",");
//            execResult = mySQLQueryManager.runTimedQueryExp(guard_query_with_choice, 1);
//            rString.append(execResult.getTimeTaken().toMillis()).append(",");
            System.out.println(rString.toString());
            if(qe.printExplain(guard_query_without_hint).equalsIgnoreCase(qe.printExplain(guard_query_with_hint_inline))){
                System.out.println("Same plan");
            }
            else {
                System.out.println("** Explain without hint: " + qe.printExplain(guard_query_without_hint));
                System.out.println("**** Explain with hint: " + qe.printExplain(guard_query_without_hint));
                System.out.println("-----------------------------------");
            }
        }
    }

    public static void main(String[] args) {
        DesignChoice2Experiment dc2e = new DesignChoice2Experiment();
        dc2e.runExpt();
    }
}
