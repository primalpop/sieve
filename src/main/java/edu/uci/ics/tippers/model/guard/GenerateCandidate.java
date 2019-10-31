package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.util.*;

public class GenerateCandidate {

    private Map<String, List<ObjectCondition>> aMap;
    private Map<ObjectCondition, BEPolicy> oMap;


    public GenerateCandidate(BEExpression inputExp) {
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
     * @param pBar
     * @return
     */
    private boolean shouldIMerge(ObjectCondition oc1, ObjectCondition oc2, BEPolicy pBar) {
        ObjectCondition intersect = oc1.intersect(oc2);
        ObjectCondition union = oc1.union(oc2);
        double lhs = intersect.computeL() / union.computeL();
        long numOfPreds = pBar.countNumberOfPredicates();
        double rhs = (PolicyConstants.ROW_EVALUATE_COST * numOfPreds) / (PolicyConstants.IO_BLOCK_READ_COST
                + (PolicyConstants.ROW_EVALUATE_COST * numOfPreds));
        return lhs > rhs;
    }

    private ObjectCondition mergeEm(ObjectCondition oc1, ObjectCondition oc2){
        ObjectCondition subMerged = null;
        BEPolicy beM =  oMap.get(oc1).getObject_conditions().size() > oMap.get(oc2).getObject_conditions().size()?
                new BEPolicy(oMap.get(oc1)): new BEPolicy(oMap.get(oc2));
        if(shouldIMerge(oc1, oc2, beM)) subMerged = oc1.union(oc2);
        return subMerged;
    }


    /**
     * For each @param attribute,
     *      1. we collect all the predicates on that attribute
     *      2. predicates are sorted based on their left range
     *      3. for each predicate (i)
     *          3.a. for each predicate (j)
     *          3.a.1. we check if i and j overlaps; continue if it doesn't
     *          3.a.2 we check if they if they are identical and add them to eqObjs and continue the inner loop
     *          3.a.2 then, we check the merge condition and merge them if it satisfies
     *          3.a.3 the merged predicate is added to all the identical policies (stored in eqObjs)
     *          3.a.4  if not merged, update nextCount (based on the merge theorem)
     *          3.a.5  break and continue the outer loop if nextCount >=2
     *          3.b increment the outer loop by the size of eqObjs to skip over identical predicates
     *
     * @param attribute
     * @return
     */
    private void extendOnAttribute(String attribute) {
        List<ObjectCondition> preds = aMap.get(attribute);
        Collections.sort(preds);
        int mergeCount = 0;
        int identicalCount = 0;
        int i = 0;
        while(i< preds.size()){
            ObjectCondition oc1 = preds.get(i);
            int nextCount = 0;
            List<ObjectCondition> eqObjs = new ArrayList<>();
            eqObjs.add(oc1);
            for (int j = i+1; j < preds.size(); j++) {
                ObjectCondition oc2 = preds.get(j);
                if (!oc1.overlaps(oc2)) {
                    break;
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
                        if(!og.getPolicy_id().equalsIgnoreCase(oc1.getPolicy_id()) &&
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
            }
            i += eqObjs.size();
        }
//        System.out.println("Attribute: " + attribute
//                + " Merge Count " + mergeCount + " Identical Count " + identicalCount);
    }

    /**
     * for each indexed attribute which has range predicates call extendOnAttribute
     */
    public void extend(){
        for (String attrKey: aMap.keySet()) {
            if (!PolicyConstants.RANGE_ATTR_LIST.contains(attrKey)) continue;
            if(aMap.get(attrKey).size()>1) {
                extendOnAttribute(attrKey);
            }
        }
    }
}
