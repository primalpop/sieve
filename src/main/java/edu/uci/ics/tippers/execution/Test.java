package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.Histogram;
import edu.uci.ics.tippers.db.PGSQLConnectionManager;
import edu.uci.ics.tippers.db.PGSQLQueryManager;
import edu.uci.ics.tippers.db.QueryResult;
import edu.uci.ics.tippers.manager.PolicyPersistor;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.util.List;

public class Test {


    public static void main(String args[]) {

//        PGSQLConnectionManager pgsqlConnectionManager = PGSQLConnectionManager.getInstance();
//
//        PGSQLQueryManager pgsqlQueryManager = new PGSQLQueryManager();
//        QueryResult queryResult = pgsqlQueryManager.runTimedQueryExp("Select * from PRESENCE", 1);
//        System.out.println(queryResult.getTimeTaken().toMillis());

//        PolicyPersistor polper = new PolicyPersistor();
//        List<BEPolicy> bePolicyList = polper.retrievePolicies("1", PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
//        for (BEPolicy bePolicy: bePolicyList) {
//            for (ObjectCondition oc: bePolicy.getObject_conditions()) {
//                System.out.print(oc.print());
//                System.out.println(" " + oc.computeL());
//            }
//        }
    }
}
