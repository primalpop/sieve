package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.util.*;

public class PredicateMerge {

    private Map<String, List<ObjectCondition>> aMap;
    private Map<ObjectCondition, BEPolicy> oMap;


    public PredicateMerge(BEExpression inputExp) {
        oMap = new HashMap<>();
        aMap = new HashMap<>();
        for (int i = 0; i < PolicyConstants.RANGE_ATTR_LIST.size(); i++) {
            List<ObjectCondition> attrToOc = new ArrayList<>();
            String attr = PolicyConstants.RANGE_ATTR_LIST.get(i);
            aMap.put(attr, attrToOc);
        }
        constructMaps(inputExp);
    }

    private void constructMaps(BEExpression inputExp) {
        for (int i = 0; i < inputExp.getPolicies().size(); i++) {
            BEPolicy pol = inputExp.getPolicies().get(i);
            for (int j = 0; j < pol.getObject_conditions().size(); j++) {
                ObjectCondition oc = pol.getObject_conditions().get(j);
                if (!PolicyConstants.RANGE_ATTR_LIST.contains(oc.getAttribute())) continue;
                aMap.get(oc.getAttribute()).add(oc);
                if (oMap.containsKey(oc)) {
                    System.out.println("Duplicate policy id or object condition id");
                }
                oMap.put(oc, pol);
            }
        }
    }

    /**
     * TODO: Check if this matches with the paper definition
     * @param oc1
     * @param oc2
     * @param beMerged
     * @return
     */
    private boolean shouldIMerge(ObjectCondition oc1, ObjectCondition oc2, BEExpression beMerged) {
        ObjectCondition intersect = oc1.intersect(oc2);
        ObjectCondition union = oc1.union(oc2);
        double lhs = intersect.computeL() / union.computeL();
        long numOfPreds = beMerged.getPolicies().stream().mapToInt(BEPolicy::countNumberOfPredicates).sum();
        double rhs = (PolicyConstants.ROW_EVALUATE_COST * numOfPreds) / (PolicyConstants.IO_BLOCK_READ_COST
                + PolicyConstants.ROW_EVALUATE_COST + (PolicyConstants.ROW_EVALUATE_COST * numOfPreds));
        return lhs > rhs;
    }

    private ObjectCondition mergeEm(ObjectCondition oc1, ObjectCondition oc2){
        ObjectCondition subMerged = null;
        BEExpression beM = new BEExpression();
        beM.getPolicies().add(oMap.get(oc1));
        beM.getPolicies().add(oMap.get(oc2));
        if(shouldIMerge(oc1, oc2, beM)) subMerged = oc1.union(oc2);
        return subMerged;
    }


    /**
     * For each @param attribute,
     *      1. we collect all the predicates on that attribute
     *      2. predicates are sorted based on their left range
     *      3. for each predicate (i)
     *          3.a. we retrieve the i+1 predicate and check if it satisfies merge condition
     *          3.b. then, merge it and add it to the original policies, update i = merged predicate
     *          3.c  else update nextCount and check if it's greater than 2
     *
     * @param attribute
     * @return
     */
    private void extendOnAttribute(String attribute) {
        List<ObjectCondition> preds = aMap.get(attribute);
        Collections.sort(preds);
        int mergeCount = 0;
        int identicalCount = 0;
        for (int i = 0; i < preds.size(); i++) {
            ObjectCondition oc1 = preds.get(i);
            int nextCount = 0;
            List<ObjectCondition> eqObjs = new ArrayList<>();
            eqObjs.add(oc1);
            for (int j = i+1; j < preds.size(); j++) {
                ObjectCondition oc2 = preds.get(j);
                if (!oc1.overlaps(oc2)) {
                    continue;
                }
                if(oc1.equalsWithoutId(oc2)) {
                    identicalCount += 1;
                    eqObjs.add(oc2);
                }
                ObjectCondition cMerged = mergeEm(oc1, oc2);
                if (cMerged != null) {
                    mergeCount += 1;
                    BEPolicy mPolicy = new BEPolicy();
                    mPolicy.getObject_conditions().addAll(oMap.get(oc1).getObject_conditions());
                    mPolicy.getObject_conditions().addAll(oMap.get(oc2).getObject_conditions());
                    if(!oMap.get(oc1).containsObjCond(cMerged)) oMap.get(oc1).getObject_conditions().add(cMerged);
                    if(!oMap.get(oc2).containsObjCond(cMerged)) oMap.get(oc2).getObject_conditions().add(cMerged);
                    oMap.put(cMerged, mPolicy);
                    for (ObjectCondition og: eqObjs) {
                        if(!og.getPolicy_id().equalsIgnoreCase(oc1.getPolicy_id()) ||
                                !og.getPolicy_id().equalsIgnoreCase(oc2.getPolicy_id())) {
                            if (!oMap.get(og).containsObjCond(cMerged))
                                oMap.get(og).getObject_conditions().add(cMerged);
                        }
                    }
                    oc1 = cMerged;
                }
                else{
                    nextCount += 1;
                    if(nextCount >= 2) {
                        break;
                    }
                }
                i+= eqObjs.size();
            }
        }
        System.out.println("Attribute: " + attribute
                + " Merge Count " + mergeCount + " Identical Count " + identicalCount);
    }

    public void extend(){
        for (String attrKey: aMap.keySet()) {
            if (!PolicyConstants.RANGE_ATTR_LIST.contains(attrKey)) continue;
            if(aMap.get(attrKey).size()>1) {
                extendOnAttribute(attrKey);
            }
        }
    }
}
