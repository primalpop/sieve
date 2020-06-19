package edu.uci.ics.tippers.execution.experiments.design;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.dbms.QueryManager;
import edu.uci.ics.tippers.dbms.QueryResult;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.generation.policy.WiFiDataSet.PolicyUtil;
import edu.uci.ics.tippers.persistor.GuardPersistor;
import edu.uci.ics.tippers.persistor.PolicyPersistor;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.model.guard.GuardPart;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.TimeStampPredicate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DesignChoicesExperiment {

    QueryManager queryManager = new QueryManager();
    boolean guardTO = false, queryTO = false, sieveTO = false;


    private String runExpt(GuardExp guardExp, String queryPredicates, boolean guard_hint){
        StringBuilder rString = new StringBuilder();
        String guard_query_with_hint_inline = guardExp.inlineRewrite(true);
        guard_query_with_hint_inline += "Select * from polEval where " + queryPredicates;
        String query_hint = "date_tree";
        String guard_query_with_hint_query = "SELECT * from ( SELECT * from PRESENCE force index(" + query_hint
                + ") where " + queryPredicates + " ) as P where " + guardExp.createQueryWithOR();
        QueryResult execResult = null;
        if(!guardTO) {
            execResult = queryManager.runTimedQueryExp(guard_query_with_hint_inline, 3);
            rString.append(execResult.getTimeTaken().toMillis()).append(",");
            if(execResult.getTimeTaken().equals(PolicyConstants.MAX_DURATION)) guardTO = true;
        }
        else
            rString.append(PolicyConstants.MAX_DURATION.toMillis()).append(",");
        if(!queryTO) {
            execResult = queryManager.runTimedQueryExp(guard_query_with_hint_query, 3);
            rString.append(execResult.getTimeTaken().toMillis()).append(",");
            if(execResult.getTimeTaken().equals(PolicyConstants.MAX_DURATION)) queryTO = true;
        }
        else
            rString.append(PolicyConstants.MAX_DURATION.toMillis()).append(",");
        rString.append("\n");
        return rString.toString();
    }

    private String runGuardExpt(String query, List<Integer> queriers){
        PolicyPersistor polper = PolicyPersistor.getInstance();
        double querySel = queryManager.checkSelectivity(query);
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
        PolicyPersistor polper = PolicyPersistor.getInstance();
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
            double querySel = queryManager.checkSelectivity(queries.get(j));
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
     * Estimating evaluation cost of policies through different runs
     * @return
     */
    private long estimateCostOfPolicyEval(){
        long policyEvalTime = 0;
        QueryManager queryManager = new QueryManager();
        String query_predicates = ") Select * from polEval";
        String querier = "7073";
        String guard_id = "'a21c9919-6e54-4b02-90d8-cc80720833a9'";
        String guard = "WITH polEval as (Select * from PRESENCE WHERE (start_date<= \"2018-02-05\") " +
                "AND (start_date>= \"2018-02-03\")";
        String udf_query = " hybcheck(" + querier + ", "
                + guard_id +  " , user_id, location_id, start_date, " +
                "start_time, user_profile, user_group ) = 1";
        QueryResult udfTime = queryManager.runTimedQueryExp(guard + PolicyConstants.CONJUNCTION
                + udf_query + query_predicates, 3);
        System.out.println("UDF time " + udfTime.getTimeTaken().toMillis());
        String add_policy_1 = "((user_profile= \"faculty\") " +
                "AND (start_date<= \"2018-04-30\") AND (start_date>= \"2018-02-28\") " +
                "AND (start_time>= \"00:10:00\") AND (start_time<= \"10:59:00\"))";
        String add_policy_2 = "((user_profile= \"graduate\") " +
                "AND (location_id = \"3145-clwa-5231\")" +
                "AND (start_date<= \"2018-03-30\") AND (start_date>= \"2018-02-15\") " +
                "AND (start_time>= \"15:12:00\") AND (start_time<= \"23:59:00\"))";

        for (int i = 8; i < 200; i=i+10) {
            String query = guard + " AND ((user_profile= \"graduate\") " +
                    "AND (start_date<= \"2018-05-01\") AND (start_date>= \"2018-02-28\") " +
                    "AND (start_time>= \"08:00:00\") AND (start_time<= \"17:00:00\")) " +
                    "OR ((location_id = \"3146-clwa-6217\") AND (start_date<= \"2018-01-30\") " +
                    "AND (start_date>= \"2018-02-01\") AND (start_time>= \"12:00:00\") AND (start_time<= \"17:00:00\"))  ";
            for (int j = 0; j < i; j++)
                if(j%2 == 0)
                    query += PolicyConstants.DISJUNCTION + add_policy_1;
                else
                    query += PolicyConstants.DISJUNCTION + add_policy_2;
            query += query_predicates;
            QueryResult execTime = queryManager.runTimedQueryExp(query, 3);
            policyEvalTime = execTime.getTimeTaken().toMillis();
            System.out.println(" Num of " + i + " policies: " + execTime.getTimeTaken().toMillis());
        }
        return policyEvalTime;
    }


    /**
     * Generation of queries with increasing order of cardinalities
     * @return
     */
    private List<String> generateQueries(){
        List<String> queries = new ArrayList<>();
        PolicyUtil pg = new PolicyUtil();
        for (int j =0; j < 24; j++) {
            TimeStampPredicate tsPred = new TimeStampPredicate(pg.getDate("MIN"), 0, "00:00", 60*j);
            String query = String.format("start_time >= \"%s\" AND start_time <= \"%s\" ", tsPred.getStartTime().toString(),
                    tsPred.getEndTime().toString());
            query += String.format("and start_date >= \"%s\" AND start_date <= \"%s\" ", tsPred.getStartDate().toString(),
                    tsPred.getEndDate().toString());
            queries.add(query);
        }
        int duration =  1439; //maximum of a day
        for (int i = 0; i < 20; i=i+2) {
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
     */
    public void runExperiment() {
        DesignChoicesExperiment dc2e = new DesignChoicesExperiment();
        String filename = "expts2_calibration.csv";
        String file_header = "Guard Cardinality,Query Cardinality,With Guard Hint Inline,With Query Hint \n";
        Writer writer = new Writer();
        writer.writeString(file_header, PolicyConstants.EXP_RESULTS_DIR, filename);
        List<String> queries = dc2e.generateQueries();
        //Queriers with <low, medium, high> guard cardinalities
        //From Sheet: Copy of guard analysis
        List<Integer> queriers = new ArrayList<>(Arrays.asList(31398, 16439, 10727, 22995,
                30528, 6213, 7964, 2039,
                34035, 11506, 24101, 18094,
                11695, 18289, 3980,	32467,
                9661, 10473, 14677, 22636,
                9892, 19987, 12225, 15007));
        List<Integer> rep_queriers = new ArrayList<>(queriers);
        //Running Query Experiment with three guard cardinalities
        for (int i = 0; i < rep_queriers.size(); i++) {
            writer.writeString(dc2e.runQueryExpt(String.valueOf(rep_queriers.get(i)), queries), PolicyConstants.EXP_RESULTS_DIR,
                    filename);
            dc2e.resetFlags();
        }

//        //Queries with <low, medium, high> cardinalities
//        List<String> rep_queries = new ArrayList<>(Arrays.asList(queries.get((int) Math.ceil(queries.size()/10.0)),
//                queries.get((int) Math.ceil(queries.size()/5.0)), queries.get((int) Math.ceil(queries.size()/2.0))));
//        for (int i = 0; i < rep_queries.size(); i++) {
//            writer.writeString(dc2e.runGuardExpt(rep_queries.get(i), queriers), PolicyConstants.BE_POLICY_DIR,
//                    filename);
//            dc2e.resetFlags();
//        }
    }
}
