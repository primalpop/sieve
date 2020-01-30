package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.db.MySQLResult;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.generation.policy.PolicyGen;
import edu.uci.ics.tippers.generation.query.QueryExplainer;
import edu.uci.ics.tippers.manager.GuardPersistor;
import edu.uci.ics.tippers.manager.PolicyPersistor;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.model.guard.GuardPart;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.TimeStampPredicate;

import java.security.Policy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DesignChoice2Experiment {

    MySQLQueryManager mySQLQueryManager = new MySQLQueryManager();

    private String runExpt(String querier, List<String> queries){
        QueryExplainer qe = new QueryExplainer();
        PolicyPersistor polper = new PolicyPersistor();
        boolean guardTO = false, queryTO = false, sieveTO = false;
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
        StringBuilder finalString = new StringBuilder();
        for (int j = 0; j < queries.size(); j++) {
            StringBuilder rString = new StringBuilder();
            double querySel = mySQLQueryManager.checkSelectivity(queries.get(j));
            rString.append(totalCard).append(",");
            rString.append(querySel).append(",");
            double k = querySel/(totalCard/(guardExp.getGuardParts().size()));
            boolean guard_hint = false;
            if (k > guardExp.getGuardParts().size()) guard_hint = true;
            String queryPredicates = queries.get(j);
            String guard_query_with_hint_inline = guardExp.inlineRewrite(true);
            guard_query_with_hint_inline += "Select * from polEval where " + queryPredicates;
            String query_hint = "date_tree";
            String guard_query_with_hint_query = "SELECT * from ( SELECT * from PRESENCE force index(" + query_hint
                    + ") where " + queryPredicates + " ) as P where " + guardExp.createQueryWithOR();
            String guard_query_optimal = null;
            if(guard_hint)
                guard_query_optimal = guard_query_with_hint_inline;
            else
                guard_query_optimal = guard_query_with_hint_query;
//            String guard_query_without_hint = guardExp.rewriteWithoutHint();
//            guard_query_without_hint += "Select * from polEval where " + queryPredicates;
//            String guard_query_with_choice = guardExp.inlineOrNot(true);
//            guard_query_with_choice += "Select * from polEval where " + queryPredicates;
            MySQLResult execResult = null;
            if(!guardTO) {
                execResult = mySQLQueryManager.runTimedQueryExp(guard_query_with_hint_inline, 1);
                rString.append(execResult.getTimeTaken().toMillis()).append(",");
                if(execResult.getTimeTaken().equals(PolicyConstants.MAX_DURATION)) guardTO = true;
            }
            else
                rString.append(PolicyConstants.MAX_DURATION.toMillis()).append(",");
            if(!queryTO) {
                execResult = mySQLQueryManager.runTimedQueryExp(guard_query_with_hint_query, 1);
                rString.append(execResult.getTimeTaken().toMillis()).append(",");
                if(execResult.getTimeTaken().equals(PolicyConstants.MAX_DURATION)) queryTO = true;
            }
            else
                rString.append(PolicyConstants.MAX_DURATION.toMillis()).append(",");
            if(!sieveTO){
                execResult = mySQLQueryManager.runTimedQueryExp(guard_query_optimal, 1);
                rString.append(execResult.getTimeTaken().toMillis()).append(",");
                if(execResult.getTimeTaken().equals(PolicyConstants.MAX_DURATION)) sieveTO = true;
                rString.append(guard_hint);
            }
            else
                rString.append(PolicyConstants.MAX_DURATION.toMillis()).append(",");
            rString.append("\n");
            finalString.append(rString);
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
        return finalString.toString();
    }

    public static void main(String[] args) {
        DesignChoice2Experiment dc2e = new DesignChoice2Experiment();
        String file_header = "Guard Cardinality,Query Cardinality,Without Hint,With Guard Hint Inline,With Query Hint,Our Approach,flag";
        Writer writer = new Writer();
        writer.writeString(file_header, PolicyConstants.BE_POLICY_DIR, "expts2.csv");
        List<String> queries = new ArrayList<>();
        PolicyGen pg = new PolicyGen();
        for (int j =0; j < 24; j++) {
            TimeStampPredicate tsPred = new TimeStampPredicate(pg.getDate("MIN"), 0, "00:00", 60*j);
            String query = String.format("start_time >= \"%s\" AND start_time <= \"%s\" ", tsPred.getStartTime().toString(),
                    tsPred.getEndTime().toString());
            query += String.format("and start_date >= \"%s\" AND start_date <= \"%s\" ", tsPred.getStartDate().toString(),
                    tsPred.getEndDate().toString());
            queries.add(query);
        }
        int duration =  1439; //maximum of a day
        for (int i = 0; i < 90; i=i+3) {
            TimeStampPredicate tsPred = new TimeStampPredicate(pg.getDate("MIN"), i, "00:00", duration);
            String query = String.format("start_date >= \"%s\" AND start_date <= \"%s\" ", tsPred.getStartDate().toString(),
                    tsPred.getEndDate().toString());
            queries.add(query);
        }

        List<Integer> queriers = new ArrayList<>(Arrays.asList(29360, 18770, 15039, 22636));

        Experiment e = new Experiment();
        for (int i = 0; i < queriers.size(); i++) {
            writer.writeString(dc2e.runExpt(String.valueOf(queriers.get(i)), queries), PolicyConstants.BE_POLICY_DIR,
                    "expts2.csv");
        }
    }
}
