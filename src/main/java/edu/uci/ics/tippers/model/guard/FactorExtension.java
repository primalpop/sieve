package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.util.*;
import java.util.stream.Collectors;


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
        this.genExpression = genExpression;
        oMap = new HashMap<>();
        aMap = new HashMap<>();
        replacementMap = new HashMap<>();
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
        long numOfPreds = beMerged.getPolicies().stream().map(BEPolicy::getObject_conditions).mapToInt(List::size).sum();
        double rhs = (PolicyConstants.ROW_EVALUATE_COST * numOfPreds) / (PolicyConstants.IO_BLOCK_READ_COST
                + PolicyConstants.ROW_EVALUATE_COST + (PolicyConstants.ROW_EVALUATE_COST * numOfPreds) );
        return lhs > rhs;
    }

    /**
     * TODO: In the case of identical predicates does it include savings from reading only once?
     * Estimates the cost of a evaluating the policy with an index on Object condition 'oc'
     * Selectivity of oc * D * Index access + Selectivity of oc * D * cost of filter * alpha * number of predicates
     * alpha (set to 2/3) is a parameter which determines the number of predicates that are evaluated in the policy
     * @return
     */
    public double estimateCost(ObjectCondition oc, BEExpression beExp){
        long numOfPreds = beExp.getPolicies().stream().map(BEPolicy::getObject_conditions).mapToInt(List::size).sum();
        return PolicyConstants.NUMBER_OR_TUPLES * oc.computeL() * (PolicyConstants.IO_BLOCK_READ_COST *  +
                PolicyConstants.ROW_EVALUATE_COST * 2 * numOfPreds * PolicyConstants.NUMBER_OF_PREDICATES_EVALUATED);
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
            if(oc.compareTo(ock) == 0){ //TODO: DELETE LATER
                if (oc.getPolicy_id().equalsIgnoreCase(ock.getPolicy_id())){
                    System.out.println("DEBUG!!!");
                }
            }
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

    /**
     * Returns the number of extensions performed for the expression
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
                if(m1.compareTo(m2) == 0) { //TODO: SANITY CHECK, DELETE AFTERWARDS
                    if(m1.getPolicy_id().equalsIgnoreCase(m2.getPolicy_id())){
                        System.out.println(m1.toString());
                        System.out.println(m2.toString());
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
                if(!aMap.get(m2.getAttribute()).remove(m2)){
                    throw new PolicyEngineException(m1.toString() + " not removed from aMap");
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
                //add merged oc to aMap
                aMap.get(ocM.getAttribute()).add(ocM);
            }
        }
        //Rewriting the original expression
        for(ObjectCondition original:replacementMap.keySet()) {
            this.genExpression.replenishFromPolicies(original, replacementMap.get(original));
        }
        return numberOfExtensions;
    }
}
