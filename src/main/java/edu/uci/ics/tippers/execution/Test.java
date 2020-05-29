package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.QueryManager;
import edu.uci.ics.tippers.db.QueryResult;
import edu.uci.ics.tippers.manager.PolicyPersistor;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.model.guard.SelectGuard;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

public class Test {


    public static void main(String args[]) {

        PolicyConstants.initialize();

//        QueryManager queryManager = new QueryManager();
//        QueryResult queryResult = queryManager.runTimedQueryExp("Select * from ORDERS", 1);
//        System.out.println(queryResult.getResultCount());

//
//        String querier = "177";
//        PolicyPersistor polper = new PolicyPersistor();
//        List<BEPolicy> bePolicyList = polper.retrievePolicies(querier, PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
//        BEExpression beExpression = new BEExpression(bePolicyList);
////        String polEvalQuery = "With polEval as ( Select * from PRESENCE where "
////                + beExpression.createQueryFromPolices() + "  )" ;
////        System.out.println(polEvalQuery);
////
//////        for (BEPolicy bePolicy: bePolicyList) {
//////            for (ObjectCondition oc: bePolicy.getObject_conditions()) {
//////                System.out.print(oc.print());
//////                System.out.println(" " + oc.computeL());
//////            }
//////        }
////
//        System.out.println("Querier #: " + querier + " with " + bePolicyList.size() + " allow policies");
//        BEExpression allowBeExpression = new BEExpression(bePolicyList);
//        Duration guardGen = Duration.ofMillis(0);
//        Instant fsStart = Instant.now();
//        SelectGuard gh = new SelectGuard(allowBeExpression, true);
////        SelectGuard gh = new SelectGuard(allowBeExpression, true, PolicyConstants.TPCH_ORDERS_ATTR_LIST,
////                PolicyConstants.TPCH_ORDERS_RANGE_ATTR_LIST, PolicyConstants.TPCH_ORDERS_ATTRIBUTE_IND);
//        Instant fsEnd = Instant.now();
//        System.out.println(gh.createGuardedQuery(true));
//        guardGen = guardGen.plus(Duration.between(fsStart, fsEnd));
//        System.out.println("Guard Generation time: " + guardGen + " Number of Guards: " + gh.numberOfGuards());
//        System.out.println(gh.create(querier, "user").createQueryWithUnion());
//
//        GuardExp ge = gh.create(querier, "user");
//        for (int i = 0; i < ge.getGuardParts().size(); i++) {
//            System.out.println(ge.getGuardParts().get(i).getGuard().print());
//        }

//        guardPersistor.insertGuard(gh.create(String.valueOf(querier), "user"));


    }
}
