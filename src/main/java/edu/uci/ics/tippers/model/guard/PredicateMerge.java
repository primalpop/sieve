package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.util.*;

public class PredicateMerge {

    BEExpression genExpression;

    Map<String, List<ObjectCondition>> aMap;
    Map<ObjectCondition, List<BEPolicy>> oMap;


    public PredicateMerge() {
        oMap = new HashMap<>();
        aMap = new HashMap<>();
        for (int i = 0; i < PolicyConstants.RANGE_ATTR_LIST.size(); i++) {
            List<ObjectCondition> attrToOc = new ArrayList<>();
            String attr = PolicyConstants.RANGE_ATTR_LIST.get(i);
            aMap.put(attr, attrToOc);
        }
        constructMaps();
    }

    private void constructMaps() {
        for (int i = 0; i < genExpression.getPolicies().size(); i++) {
            BEPolicy pol = genExpression.getPolicies().get(i);
            for (int j = 0; j < pol.getObject_conditions().size(); j++) {
                ObjectCondition oc = pol.getObject_conditions().get(j);
                if (!PolicyConstants.RANGE_ATTR_LIST.contains(oc.getAttribute())) continue;
                aMap.get(oc.getAttribute()).add(oc);
                if (oMap.containsKey(oc)) {
                    oMap.get(oc).add(pol);
                } else {
                    List<BEPolicy> bePolicies = new ArrayList<>();
                    bePolicies.add(pol);
                    oMap.put(oc, bePolicies);
                }
            }
        }
    }

    private boolean shouldIMerge(ObjectCondition oc1, ObjectCondition oc2, BEExpression beMerged) {
        if (!oc1.overlaps(oc2)) return false;
        ObjectCondition intersect = oc1.intersect(oc2);
        ObjectCondition union = oc1.union(oc2);
        double lhs = intersect.computeL() / union.computeL();
        long numOfPreds = beMerged.getPolicies().stream().mapToInt(BEPolicy::countNumberOfPredicates).sum();
        double rhs = (PolicyConstants.ROW_EVALUATE_COST * numOfPreds) / (PolicyConstants.IO_BLOCK_READ_COST
                + PolicyConstants.ROW_EVALUATE_COST + (PolicyConstants.ROW_EVALUATE_COST * numOfPreds));
        return lhs > rhs;
    }


    private List<ObjectCondition> extendOnAttribute(String attribute) {
        List<ObjectCondition> preds = aMap.get(attribute);
        List<ObjectCondition> pGuards = new ArrayList<>();
        Collections.sort(preds);
        for (int i = 0; i < preds.size(); i++) {
            ObjectCondition puc = preds.get(i);
            ObjectCondition cp = preds.get(i+1);
            ObjectCondition cMerged = preds.get(i);
            BEExpression beM = new BEExpression();
            beM.getPolicies().addAll(oMap.get(puc));
            beM.getPolicies().addAll(oMap.get(cp));
            if(shouldIMerge(puc, cp, beM)) {
                cMerged = puc.union(cp);
                pGuards.add(cMerged);
            }
            else{
                ObjectCondition peek = preds.get(i+2);
                BEExpression mBxp = new BEExpression();
                beM.getPolicies().addAll(oMap.get(puc));
                beM.getPolicies().addAll(oMap.get(peek));
                if(shouldIMerge(puc, peek, mBxp)) {

                }
            }

        }

    }
}
