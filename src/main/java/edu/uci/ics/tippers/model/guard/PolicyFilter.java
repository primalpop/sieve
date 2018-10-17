package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

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
        int repetitions = 10;
        Duration rcost = Duration.ofMillis(0);
        for (ObjectCondition kOb : gMap.keySet()) {
            List<Long> cList = new ArrayList<>();
            for (int i = 0; i < repetitions; i++) {
                Duration tCost = mySQLQueryManager.runTimedQuery(createQueryFromGQ(kOb, gMap.get(kOb)));
                cList.add(tCost.toMillis());
            }
            Collections.sort(cList);
            List<Long> clippedList = cList.subList(1, 9);
            Duration gCost = Duration.ofMillis(clippedList.stream().mapToLong(i-> i).sum()/clippedList.size());
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
