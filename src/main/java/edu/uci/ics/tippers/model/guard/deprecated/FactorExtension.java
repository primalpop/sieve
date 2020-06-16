package edu.uci.ics.tippers.model.guard.deprecated;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import edu.uci.ics.tippers.dbms.mysql.Histogram;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.util.*;


/**
 * Assumption for cost estimation: all attributes in the relation are indexed
 *
 */
public class FactorExtension {

    BEExpression genExpression;

    Map<String, List<ObjectCondition>> aMap;
    Map<ObjectCondition, List<BEPolicy>> oMap;
    Map<ObjectCondition, ObjectCondition> replacementMap;

    public BEExpression getGenExpression() {
        return genExpression;
    }

    public FactorExtension(BEExpression genExpression) {
        Histogram.getInstance();
        this.genExpression = genExpression;
        oMap = new HashMap<>();
        aMap = new HashMap<>();
        replacementMap = new HashMap<>();
        for (int i = 0; i < PolicyConstants.RANGED_ATTRIBUTES.size(); i++) {
            List<ObjectCondition> attrToOc = new ArrayList<>();
            String attr = PolicyConstants.RANGED_ATTRIBUTES.get(i);
            aMap.put(attr, attrToOc);
        }
        constructMaps();
    }

    private void constructMaps() {
        for (int i = 0; i < genExpression.getPolicies().size(); i++) {
            BEPolicy pol = genExpression.getPolicies().get(i);
            for (int j = 0; j < pol.getObject_conditions().size(); j++) {
                ObjectCondition oc = pol.getObject_conditions().get(j);
                if(!PolicyConstants.RANGED_ATTRIBUTES.contains(oc.getAttribute())) continue;
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

    /**
     * Checks the following merging criterion
     * intersection/union > (row_eval_cost * number of predicates) / (io_read_cost + row_eval_cost + row_eval_cost * number of predicates)
     * @param oc1
     * @param oc2
     * @param beMerged
     * @return
     */
    private boolean shouldIMerge(ObjectCondition oc1, ObjectCondition oc2, BEExpression beMerged){
        if(!oc1.overlaps(oc2)) return false;
        ObjectCondition intersect = oc1.intersect(oc2);
        ObjectCondition union = oc1.union(oc2);
        double lhs = intersect.computeL() / union.computeL();
        long numOfPreds = beMerged.getPolicies().stream().mapToInt(BEPolicy::countNumberOfPredicates).sum();
        double rhs = (PolicyConstants.ROW_EVALUATE_COST * numOfPreds) / (PolicyConstants.IO_BLOCK_READ_COST
                + PolicyConstants.ROW_EVALUATE_COST + (PolicyConstants.ROW_EVALUATE_COST * numOfPreds) );
        return lhs > rhs;
    }


    /**
     * TODO: Should we memoize object conditions that do not overlap with each other?
     * Memoizes the benefit of merging two object conditions on the same attribute
     * The memoized matrix is upper triangular i.e all the entries below the diagonal are not computed as merging is commutative
     * @param oc
     * @param lindex: -1 for elements that are not in the list
     */
    private void memoize(Map<String, Double> memoized, ObjectCondition oc, int lindex){
        for (int k = lindex + 1; k < aMap.get(oc.getAttribute()).size(); k++) {
            ObjectCondition ock = aMap.get(oc.getAttribute()).get(k);
            BEExpression beM = new BEExpression();
            beM.getPolicies().addAll(oMap.get(oc));
            beM.getPolicies().addAll(oMap.get(ock));
            if(!shouldIMerge(oc, ock, beM)) continue;
            double mBenefit = beM.estimateCostOfGuardRep(oc.union(ock), false) -
                    (new BEExpression(oMap.get(oc)).estimateCostForSelection(false)
                    + new BEExpression(oMap.get(ock)).estimateCostForSelection(false));
            memoized.put(oc.hashCode() + "." + ock.hashCode(), mBenefit);
        }
    }


    private void removeFromMemoized(Map<String, Double> memoized, ObjectCondition oc){
        List<String> removal = new ArrayList<>();
        for (String memKey: memoized.keySet()){
            if (oc.hashCode() == Integer.parseInt(memKey.split("\\.")[0]) ||
                    oc.hashCode() == Integer.parseInt(memKey.split("\\.")[1]))
                removal.add(memKey);
        }
        memoized.keySet().removeAll(removal);
    }

    public List<ObjectCondition> getKeysByValue(ObjectCondition value) {
        List<ObjectCondition> keys = new ArrayList<>();
        for (Map.Entry<ObjectCondition, ObjectCondition> entry : replacementMap.entrySet()) {
            if (entry.getValue().equals(value)) {
                keys.add(entry.getKey());
            }
        }
        return keys;
    }

    private void smartReplace(ObjectCondition orgOc, ObjectCondition repOc){
        List<ObjectCondition> orgAsValue = getKeysByValue(orgOc);
        if(orgAsValue.size() == 0) replacementMap.put(orgOc, repOc);
        else {
            for (int i = 0; i < orgAsValue.size(); i++) {
                replacementMap.put(orgAsValue.get(i), repOc);
            }
        }
    }

//    /**
//     * Returns the number of extensions performed for the expression
//     * Includes only final version of the extended predicate
//     * @return
//     */
//    public int doYourThing() {
//        int numberOfExtensions = 0;
//        for (String attribute: aMap.keySet()) {
//            List<ObjectCondition> attrToOc = aMap.get(attribute);
//            Map<String, Double> memoized = new HashMap<>();
//            for (int j = 0; j < attrToOc.size(); j++) {
//                memoize(memoized, attrToOc.get(j), j);
//            }
//            while(true){
//                if(memoized.size() == 0) break;
//                Map.Entry<String, Double> maxBenefitKey = memoized.entrySet().stream()
//                        .max(Map.Entry.comparingByValue()).get();
//                if(maxBenefitKey.getValue() < 0) break;
//                ObjectCondition m1 = null;
//                ObjectCondition m2 = null;
//                for (ObjectCondition g: attrToOc) {
//                    if(m1 != null && m2 != null) break;
//                    if (g.hashCode() == Integer.parseInt(maxBenefitKey.getKey().split("\\.")[0])) m1 = g;
//                    if (g.hashCode() == Integer.parseInt(maxBenefitKey.getKey().split("\\.")[1])) m2 = g;
//                }
//                Boolean deleteOne = false;
//                if(m1.compareTo(m2) == 0) { //TODO: SANITY CHECK, DELETE AFTERWARDS
//                    if(m1.getPolicy_id().equalsIgnoreCase(m2.getPolicy_id())){
//                        deleteOne = true;
//                    }
//                }
//                ObjectCondition ocM = m1.union(m2);
//                BEExpression beM = new BEExpression();
//                beM.getPolicies().addAll(oMap.get(m1));
//                beM.getPolicies().addAll(oMap.get(m2));
//                numberOfExtensions += 1;
//                //remove from aMap
//                if(!aMap.get(m1.getAttribute()).remove(m1)) {
//                    throw new PolicyEngineException(m1.toString() + " not removed from aMap");
//                }
//                if(!deleteOne) {
//                    if(!aMap.get(m2.getAttribute()).remove(m2)){
//                        throw new PolicyEngineException(m1.toString() + " not removed from aMap");
//                    }
//                }
//                //remove from oMap
//                oMap.remove(m1);
//                oMap.remove(m2);
//                //remove from memoized
//                removeFromMemoized(memoized, m1);
//                removeFromMemoized(memoized, m2);
//                //add merged oc to oMap
//                oMap.put(ocM, beM.getPolicies());
//                //memoize the new object condition benefit
//                memoize(memoized, ocM, -1);
//                //add to replacement map
//                smartReplace(m1, ocM);
//                smartReplace(m2, ocM);
//                //add merged oc to aMap
//                aMap.get(ocM.getAttribute()).add(ocM);
//            }
//        }
//        //Rewriting the original expression
//        for(ObjectCondition original:replacementMap.keySet()) {
//            this.genExpression.replenishFromPolicies(original, replacementMap.get(original));
//        }
//        return numberOfExtensions;
//    }

    /**
     * Returns the number of extensions performed for the expression
     * Includes intermediate versions of the merged predicate
     * @return
     */
    public int doYourThing() {
        int numberOfExtensions = 0;
        for (String attribute: aMap.keySet()) {
            List<ObjectCondition> attrToOc = aMap.get(attribute);
            Map<String, Double> memoized = new HashMap<>();
            for (int j = 0; j < attrToOc.size(); j++) {
                memoize(memoized, attrToOc.get(j), j);
            }
            while(true){
                if(memoized.size() == 0) break;
                Map.Entry<String, Double> maxBenefitKey = memoized.entrySet().stream()
                        .max(Map.Entry.comparingByValue()).get();
                if(maxBenefitKey.getValue() < 0) break;
                ObjectCondition m1 = null;
                ObjectCondition m2 = null;
                for (ObjectCondition g: attrToOc) {
                    if(m1 != null && m2 != null) break;
                    if (g.hashCode() == Integer.parseInt(maxBenefitKey.getKey().split("\\.")[0])) m1 = g;
                    if (g.hashCode() == Integer.parseInt(maxBenefitKey.getKey().split("\\.")[1])) m2 = g;
                }
                boolean deleteOne = false;
                if (m2 != null && m1 != null && m1.compareTo(m2) == 0) { //TODO: SANITY CHECK, DELETE AFTERWARDS
                    if (m1.getPolicy_id().equalsIgnoreCase(m2.getPolicy_id())) {
                        deleteOne = true;
                    }
                }
                ObjectCondition ocM = m1.union(m2);
                BEExpression beM = new BEExpression();
                beM.getPolicies().addAll(oMap.get(m1));
                beM.getPolicies().addAll(oMap.get(m2));
                numberOfExtensions += 1;
                //remove from aMap
                if(!aMap.get(m1.getAttribute()).remove(m1)) {
                    throw new PolicyEngineException(m1.toString() + " not removed from aMap");
                }
                if(!deleteOne) {
                    if(!aMap.get(m2.getAttribute()).remove(m2)){
                        throw new PolicyEngineException(m1.toString() + " not removed from aMap");
                    }
                }
                //remove from oMap
                oMap.remove(m1);
                oMap.remove(m2);
                //remove from memoized
                removeFromMemoized(memoized, m1);
                removeFromMemoized(memoized, m2);
                //add merged oc to oMap
                oMap.put(ocM, beM.getPolicies());
                //memoize the new object condition benefit
                memoize(memoized, ocM, -1);
                //add to replacement map
                smartReplace(m1, ocM);
                smartReplace(m2, ocM);
                this.genExpression.replenishFromPolicies(m1, ocM);
                this.genExpression.replenishFromPolicies(m2, ocM);
                //add merged oc to aMap
                aMap.get(ocM.getAttribute()).add(ocM);
            }
        }
        return numberOfExtensions;
    }
}
