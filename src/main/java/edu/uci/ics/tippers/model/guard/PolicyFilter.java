package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.time.Duration;
import java.util.HashMap;

public class PolicyFilter {

    BEExpression expression;

    MySQLQueryManager mySQLQueryManager = new MySQLQueryManager();


    public PolicyFilter(BEExpression beExpression){
        this.expression = beExpression;
    }

    public Duration computeCost(){
        HashMap<ObjectCondition, BEExpression> pFilterMap = new HashMap<>();
        for (BEPolicy bePolicy : this.expression.getPolicies()) {
            double freq = PolicyConstants.NUMBER_OR_TUPLES;
            ObjectCondition gOC = new ObjectCondition();
            for (ObjectCondition oc : bePolicy.getObject_conditions()) {
                if (!PolicyConstants.INDEXED_ATTRS.contains(oc.getAttribute())) continue;
                if (oc.computeL() < freq) {
                    freq = oc.computeL();
                    gOC = oc;
                }
            }
            BEExpression quo = new BEExpression();
            quo.getPolicies().add(bePolicy);
            pFilterMap.put(gOC, quo);
        }
        return computeGuardCosts(pFilterMap);
    }


    public Duration computeGuardCosts(HashMap<ObjectCondition, BEExpression> gMap) {
        Duration rcost = Duration.ofNanos(0);
        for (ObjectCondition kOb : gMap.keySet()) {
            Duration gCost = mySQLQueryManager.runTimedQuery(createQueryFromGQ(kOb, gMap.get(kOb)));
            rcost = rcost.plus(gCost);
        }
        return rcost;
    }

    public String createQueryFromGQ(ObjectCondition guard, BEExpression partition) {
        StringBuilder query = new StringBuilder();
        query.append(guard.print());
        query.append(PolicyConstants.CONJUNCTION);
        query.append("(");
        query.append(partition.createQueryFromPolices());
        query.append(")");
        return query.toString();
    }


}
