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
import edu.uci.ics.tippers.model.policy.TimeStampPredicate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DesignChoice2Experiment {

    MySQLQueryManager mySQLQueryManager = new MySQLQueryManager();
    boolean guardTO = false, queryTO = false, sieveTO = false;


    private String runExpt(GuardExp guardExp, String queryPredicates, boolean guard_hint){
        StringBuilder rString = new StringBuilder();
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
        return rString.toString();
    }

    private String runGuardExpt(String query, List<Integer> queriers){
        PolicyPersistor polper = new PolicyPersistor();
        double querySel = mySQLQueryManager.checkSelectivity(query);
        StringBuilder finalString = new StringBuilder();
        for (int i = 0; i < queriers.size(); i++) {
            StringBuilder rString = new StringBuilder();
            List<BEPolicy> allowPolicies = polper.retrievePolicies(String.valueOf(queriers.get(i)),
                    PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
            GuardPersistor guardPersistor = new GuardPersistor();
            GuardExp guardExp = guardPersistor.retrieveGuardExpression(String.valueOf(queriers.get(i)), "user", allowPolicies);
            System.out.println("Querier: " + queriers.get(i) + ", # Policies: " + allowPolicies.size() + ", # guards: " + guardExp.getGuardParts().size());
            double totalCard = 0.0;
            for (int j = 0; j < guardExp.getGuardParts().size(); j++) {
                GuardPart gp = guardExp.getGuardParts().get(j);
                totalCard += gp.getCardinality();
            }
            rString.append(totalCard).append(",");
            rString.append(querySel).append(",");
            double k = querySel/(totalCard/(guardExp.getGuardParts().size()));
            boolean guard_hint = false;
            if (k > guardExp.getGuardParts().size()) guard_hint = true;
            rString.append(runExpt(guardExp, query, guard_hint));
            finalString.append(rString);
            System.out.println(rString.toString());
        }
        return finalString.toString();
    }

    private String runQueryExpt(String querier, List<String> queries){
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
            rString.append(runExpt(guardExp, queryPredicates, guard_hint));
            finalString.append(rString);
            System.out.println(rString.toString());
        }
        return finalString.toString();
    }

    /**
     * Generation of queries with increasing order of cardinalities
     * @return
     */
    private List<String> generateQueries(){
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
        return queries;
    }

    private void resetFlags(){
        guardTO = false;
        queryTO = false;
        sieveTO = false;
    }

    /**
     * Experiment for testing when it is useful to force guard index versus query index for traversal
     * @param args
     */
    public static void main(String[] args) {
        DesignChoice2Experiment dc2e = new DesignChoice2Experiment();
        String filename = "expts2_final.csv";
        String file_header = "Guard Cardinality,Query Cardinality,With Guard Hint Inline,With Query Hint,Our Approach,flag \n";
        Writer writer = new Writer();
        writer.writeString(file_header, PolicyConstants.BE_POLICY_DIR, filename);
        List<String> queries = dc2e.generateQueries();
        //Queriers with increasing order of cardinalities
        List<Integer> queriers = new ArrayList<>(Arrays.asList(14215, 56, 2050, 2819, 37, 625, 23519, 8817, 6215, 387,
                945, 8962, 23416, 34035));
        //Queriers with <low, medium, high> guard cardinalities
//        List<Integer> rep_queriers = new ArrayList<>(Arrays.asList(queriers.get((int) Math.ceil(queriers.size()/10.0)),
//                queriers.get((int) Math.ceil(queriers.size()/2.0)), queriers.get(queriers.size()-1)));
//        //Running Query Experiment with three guard cardinalities
//        for (int i = 0; i < rep_queriers.size(); i++) {
//            writer.writeString(dc2e.runQueryExpt(String.valueOf(rep_queriers.get(i)), queries), PolicyConstants.BE_POLICY_DIR,
//                    filename);
//            dc2e.resetFlags();
//        }

        //Queries with <low, medium, high> cardinalities
        List<String> rep_queries = new ArrayList<>(Arrays.asList(queries.get((int) Math.ceil(queries.size()/10.0)),
                queries.get((int) Math.ceil(queries.size()/5.0)), queries.get((int) Math.ceil(queries.size()/2.0))));
        for (int i = 0; i < rep_queries.size(); i++) {
            writer.writeString(dc2e.runGuardExpt(rep_queries.get(i), queriers), PolicyConstants.BE_POLICY_DIR,
                    filename);
            dc2e.resetFlags();
        }
    }
}
