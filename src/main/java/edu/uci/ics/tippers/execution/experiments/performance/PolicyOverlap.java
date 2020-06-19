package edu.uci.ics.tippers.execution.experiments.performance;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.dbms.QueryManager;
import edu.uci.ics.tippers.dbms.QueryResult;
import edu.uci.ics.tippers.generation.policy.tpch.TPolicyGen;
import edu.uci.ics.tippers.generation.query.QueryExplainer;
import edu.uci.ics.tippers.persistor.PolicyPersistor;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.model.guard.GuardPart;
import edu.uci.ics.tippers.model.guard.SelectGuard;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Extra experiment not included in the paper
 * Scalability Testing based on ORDERS table from TPCH table which evaluates sieve performance against overlap
 **/

public class PolicyOverlap {

    private static PolicyPersistor polper;
    private static QueryManager queryManager;
    private static QueryExplainer queryExplainer;
    private static TPolicyGen tpg;

    public PolicyOverlap(){
        PolicyConstants.initialize();
        polper = PolicyPersistor.getInstance();
        queryManager = new QueryManager();
        queryExplainer = new QueryExplainer();
        tpg  = new TPolicyGen();
    }

    public static void overlapSelExperiment(List<Integer> queriers){
        double overlap = 0.005;
        for (int i = 0; i < queriers.size(); i++) {
            BEExpression beExpression = new BEExpression(polper.retrievePolicies(String.valueOf(queriers.get(i)),
                    PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW));
            if(beExpression == null) continue;
            System.out.println(beExpression.getPolicies().size() + "," + overlap + ","
                    +  (double) queryManager.runTimedQueryWithOutSorting(beExpression.createQueryFromPolices(), true).getResultCount()/PolicyConstants.getNumberOfTuples());
        }
    }

    public static void main(String args[]) {
        PolicyOverlap t2 = new PolicyOverlap();
        List<Integer> allQueriers = tpg.getAllCustomerKeys().subList(21, 42);

        System.out.println("Number of tuples: " + PolicyConstants.getNumberOfTuples());
        for (int i = 0; i < allQueriers.size(); i=i+3) {
            String querier = String.valueOf(allQueriers.get(i));
            List<BEPolicy> bePolicyList = polper.retrievePolicies(querier, PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
            if(bePolicyList == null) continue;
            BEExpression beExpression = new BEExpression(bePolicyList);
            double ben = 0.0;
            long numPreds = 0;
            for (BEPolicy bp : beExpression.getPolicies()) {
                ben += bp.estimateTableScanCost();
                numPreds += bp.countNumberOfPredicates();
            }
            System.out.println("Number of policies: " + bePolicyList.size());
//            System.out.println("Number of total predicates: " + numPreds);
//            System.out.println("Estimated Table Scan Cost (D.|P|.α.Ccpu): " + ben);
            System.out.println("Time taken for vanilla rewrite: " + queryManager.runTimedQueryWithOutSorting(beExpression.createQueryFromPolices(), true).getTimeTaken());
//            System.out.println(beExpression.createQueryFromPolices());


            Duration guardGen = Duration.ofMillis(0);
            Instant fsStart = Instant.now();
            SelectGuard gh = new SelectGuard(beExpression, true);
            Instant fsEnd = Instant.now();
            guardGen = guardGen.plus(Duration.between(fsStart, fsEnd));
//            System.out.println("Guard Generated and took " + guardGen);
            GuardExp guardExp = gh.create(querier, "user");
            double totalGuardCard = 0.0;
            Duration guardIndexScan = Duration.ofMillis(0);
            for (int j = 0; j < guardExp.getGuardParts().size(); j++) {
                GuardPart gp = guardExp.getGuardParts().get(j);
                Instant gs = Instant.now();
                totalGuardCard += queryManager.checkSelectivity(gp.getGuard().print());
                Instant ge = Instant.now();
                guardIndexScan = guardIndexScan.plus(Duration.between(gs, ge));
            }
//            System.out.println("Number of Guards: " + gh.numberOfGuards());
//            System.out.println("Total Guard Selectivity " + totalGuardCard);
            System.out.println("Average Guard Selectivity " + totalGuardCard/gh.numberOfGuards());
//            System.out.println("Guard Index Scan Time Taken: " + guardIndexScan);
//            QueryResult qr1 = queryManager.runTimedQueryWithOutSorting(gh.create(querier, "user").createQueryWithUnion(true));
            QueryResult qr2 = queryManager.runTimedQueryWithOutSorting(gh.create(querier, "user").createQueryWithUnion(false));
//            System.out.println("Time taken for guard rewrite (Union): " + qr1.getTimeTaken() + " Number of tuples " + qr1.getResultCount());
            System.out.println("Time taken for guard rewrite (Union all): " + qr2.getTimeTaken() + " Number of tuples " + qr2.getResultCount());
        }

    }
}
