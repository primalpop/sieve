package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.util.*;
import java.util.stream.Collectors;


/**
 * Assumption: all attributes in the relation are indexed
 *
 */
public class GuardGeneration {

    BEExpression genExpression;

    Map<String, List<ObjectCondition>> aMap;
    Map<ObjectCondition, List<BEPolicy>> oMap;

    public BEExpression getGenExpression() {
        return genExpression;
    }

    public GuardGeneration(BEExpression genExpression) {
        this.genExpression = genExpression;
        oMap = new HashMap<>();
        aMap = new HashMap<>();
        for (int i = 0; i < PolicyConstants.INDEXED_ATTRS.size(); i++) {
            List<ObjectCondition> attrToOc = new ArrayList<>();
            String attr = PolicyConstants.INDEXED_ATTRS.get(i);
            aMap.put(attr, attrToOc);
        }
        constructMaps();
    }

    private void constructMaps() {
        for (int i = 0; i < genExpression.getPolicies().size(); i++) {
            BEPolicy pol = genExpression.getPolicies().get(i);
            for (int j = 0; j < pol.getObject_conditions().size(); j++) {
                ObjectCondition oc = pol.getObject_conditions().get(j);
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
     * Checks the merging criterion we have derived!
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
        long numOfPreds = beMerged.getPolicies().stream().map(BEPolicy::getObject_conditions).mapToInt(List::size).sum();
        double rhs = (PolicyConstants.ROW_EVALUATE_COST * numOfPreds) / (PolicyConstants.IO_BLOCK_READ_COST
                + PolicyConstants.ROW_EVALUATE_COST + (PolicyConstants.ROW_EVALUATE_COST * numOfPreds) );
        return lhs > rhs;
    }

    /**
     * TODO: Double this cost estimation as it is returning negative for most of them
     * Estimates the cost of a evaluating the policy with an index on Object condition 'oc'
     * Selectivity of oc * D * Index access + Selectivity of oc * D * cost of filter * alpha * number of predicates
     * alpha (set to 2/3) is a parameter which determines the number of predicates that are evaluated in the policy
     * @return
     */
    public double estimateCost(ObjectCondition oc, BEExpression beExp){
        long numOfPreds = beExp.getPolicies().stream().map(BEPolicy::getObject_conditions).mapToInt(List::size).sum();
        return PolicyConstants.IO_BLOCK_READ_COST * PolicyConstants.NUMBER_OR_TUPLES * oc.computeL() +
                PolicyConstants.NUMBER_OR_TUPLES * oc.computeL() * PolicyConstants.ROW_EVALUATE_COST *
                        2 * numOfPreds * PolicyConstants.NUMBER_OF_PREDICATES_EVALUATED;
    }

    /**
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
            double mBenefit = estimateCost(oc.union(ock), beM);
            mBenefit -= estimateCost(oc, new BEExpression(oMap.get(oc))) + estimateCost(ock, new BEExpression(oMap.get(ock)));
            memoized.put(oc.hashCode() + "." + ock.hashCode(), mBenefit);
        }
    }


    private void removeFromMemoized(Map<String, Double> memoized, ObjectCondition oc){
        List<String> removal = new ArrayList<>();
        for (String memKey: memoized.keySet()){
            if (oc.hashCode() == Integer.parseInt(memKey.split("\\.")[0]) ||
                    oc.hashCode() == Integer.parseInt(memKey.split("\\.")[1])){
                removal.add(memKey);
            }
        }
        memoized.keySet().removeAll(removal);
    }

    private void smartReplace(Map<ObjectCondition, ObjectCondition> replacementMap, ObjectCondition orgOc, ObjectCondition repOc){

    }

    public void doYourThing() {
        Map<ObjectCondition, ObjectCondition> replacementMap = new HashMap<>();
        for (String attribute: aMap.keySet()) {
            List<ObjectCondition> attrToOc = aMap.get(attribute);
            Map<String, Double> memoized = new HashMap<>();
            for (int j = 0; j < attrToOc.size(); j++) {
                memoize(memoized, attrToOc.get(j), j);
            }

            //TODO: Is the break condition for this loop correct?
            while(true){
                if(memoized.size() == 0) break;
                Map.Entry<String, Double> maxBenefitKey = memoized.entrySet().stream()
                        .max(Map.Entry.comparingByValue()).get();
                if(maxBenefitKey.getValue() < 0) break;
                ObjectCondition m1 = null;
                ObjectCondition m2 = null;
                for (ObjectCondition g: oMap.keySet()) {
                    if(m1 != null && m2 != null) break;
                    if (g.hashCode() == Integer.parseInt(maxBenefitKey.getKey().split("\\.")[0])) m1 = g;
                    if (g.hashCode() == Integer.parseInt(maxBenefitKey.getKey().split("\\.")[1])) m2 = g;
                }
                ObjectCondition ocM = m1.union(m2);
                BEExpression beM = new BEExpression();
                beM.getPolicies().addAll(oMap.get(m1));
                beM.getPolicies().addAll(oMap.get(m2));
                //remove from aMap
                aMap.get(m1.getAttribute()).remove(m1);
                aMap.get(m2.getAttribute()).remove(m2);
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
                smartReplace(replacementMap, m1, ocM);
                smartReplace(replacementMap, m2, ocM);
                //add merged oc to aMap
                aMap.get(ocM.getAttribute()).add(ocM);
            }
        }
        //Rewriting the original expression
        for(ObjectCondition original:replacementMap.keySet()) {
            this.genExpression.replenishFromPolicies(original, replacementMap.get(original));
        }
    }
}
