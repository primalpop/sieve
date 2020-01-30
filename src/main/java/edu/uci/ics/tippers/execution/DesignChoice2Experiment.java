package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.db.MySQLResult;
import edu.uci.ics.tippers.generation.query.QueryExplainer;
import edu.uci.ics.tippers.manager.GuardPersistor;
import edu.uci.ics.tippers.manager.PolicyPersistor;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.model.guard.GuardPart;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.query.QueryStatement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class DesignChoice2Experiment {

    MySQLQueryManager mySQLQueryManager = new MySQLQueryManager();

    private void runExpt(String querier, List<QueryStatement> queries){
        QueryExplainer qe = new QueryExplainer();
        PolicyPersistor polper = new PolicyPersistor();

        List<BEPolicy> allowPolicies = polper.retrievePolicies(querier,
                PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
        GuardPersistor guardPersistor = new GuardPersistor();
        GuardExp guardExp = guardPersistor.retrieveGuardExpression(querier, "user", allowPolicies);
        System.out.println("Querier: " + querier + ", # Policies: " + allowPolicies.size() + ", # guards: " + guardExp.getGuardParts().size());
        GuardSelectivityExperiment gse = new GuardSelectivityExperiment();
        double totalCard = 0.0;
        for (int j = 0; j < guardExp.getGuardParts().size(); j++) {
            GuardPart gp = guardExp.getGuardParts().get(j);
            totalCard += gp.getCardinality();
        }
        System.out.println("Guard Cardinality,Query Cardinality,Without Hint,With Guard Hint Inline,With Query Hint,Our Approach");
        for (int j = 0; j <  queries.size(); j++) {
            StringBuilder rString = new StringBuilder();
            rString.append(totalCard).append(",");
            rString.append(queries.get(j).getSelectivity()).append(",");
            double k = queries.get(j).getSelectivity()/(totalCard/(guardExp.getGuardParts().size()));
            boolean guard_hint = false;
            if (k > guardExp.getGuardParts().size()) guard_hint = true;
            String queryPredicates = queries.get(j).getQuery();
            String guard_query_without_hint = guardExp.rewriteWithoutHint();
            guard_query_without_hint += "Select * from polEval where " + queryPredicates;
            String guard_query_with_hint_inline = guardExp.inlineRewrite(true);
            guard_query_with_hint_inline += "Select * from polEval where " + queryPredicates;
            String query_hint = qe.keyUsed("SELECT * from PRESENCE where " + queryPredicates);
            if (query_hint == null) query_hint = "date_tree";
            String guard_query_with_hint_query = guardExp.rewriteWithoutHint();
            guard_query_with_hint_query += "Select * from polEval force index ("
                    + query_hint + ") where " + queryPredicates;
            String guard_query_optimal = null;
            if(guard_hint)
                guard_query_optimal = guard_query_with_hint_inline;
            else
                guard_query_optimal = guard_query_with_hint_query;
//            String guard_query_with_choice = guardExp.inlineOrNot(true);
//            guard_query_with_choice += "Select * from polEval where " + queryPredicates;
            MySQLResult execResult = mySQLQueryManager.runTimedQueryExp(guard_query_without_hint, 1);
            rString.append(execResult.getTimeTaken().toMillis()).append(",");
            execResult = mySQLQueryManager.runTimedQueryExp(guard_query_with_hint_inline, 1);
            rString.append(execResult.getTimeTaken().toMillis()).append(",");
            execResult = mySQLQueryManager.runTimedQueryExp(guard_query_with_hint_query, 1);
            rString.append(execResult.getTimeTaken().toMillis()).append(",");
            execResult = mySQLQueryManager.runTimedQueryExp(guard_query_optimal, 1);
            rString.append(execResult.getTimeTaken().toMillis()).append(",");
            rString.append(guard_hint);
//            execResult =  mySQLQueryManager.runTimedQueryExp(guard_query_with_hint_udf, 1);
//            rString.append(execResult.getTimeTaken().toMillis()).append(",");
//            execResult = mySQLQueryManager.runTimedQueryExp(guard_query_with_choice, 1);
//            rString.append(execResult.getTimeTaken().toMillis()).append(",");
            System.out.println(rString.toString());
//            if(qe.printExplain(guard_query_without_hint).equalsIgnoreCase(qe.printExplain(guard_query_with_hint_inline))){
//                System.out.println("Same plan");
//            }
//            else {
//                System.out.println("** Explain without hint: " + qe.printExplain(guard_query_without_hint));
//                System.out.println("**** Explain with hint: " + qe.printExplain(guard_query_without_hint));
//                System.out.println("-----------------------------------");
//            }
        }
    }

    public static void main(String[] args) {
        DesignChoice2Experiment dc2e = new DesignChoice2Experiment();
        List<Integer> queriers = new ArrayList<>(Arrays.asList(21587, 29360, 18770, 15039, 22636));
        Experiment e = new Experiment();
        List<QueryStatement> queries = e.getQueries(1, 50);
        Comparator<QueryStatement> comp = Comparator.comparingDouble(QueryStatement::getSelectivity);
        queries.sort(comp);
        for (int i = 0; i < queriers.size(); i++) {
            dc2e.runExpt(String.valueOf(queriers.get(i)), queries);
        }
    }
}
