package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.util.*;

public class GuardGeneration {

    BEExpression genExpression;

    Map<String, List<ObjectCondition>> aMap;
    Map<ObjectCondition, List<BEPolicy>> oMap;
    Map<String, Double> memoized;

    Map<ObjectCondition, BEExpression> guardMap;

    public GuardGeneration(BEExpression genExpression) {
        this.genExpression = genExpression;
        memoized = new HashMap<>();
        oMap = new HashMap<>();
        aMap = new HashMap<>();
        for (int i = 0; i < PolicyConstants.INDEXED_ATTRS.size(); i++) {
            List<ObjectCondition> attrToOc = new ArrayList<>();
            String attr = PolicyConstants.INDEXED_ATTRS.get(i);
            aMap.put(attr, attrToOc);
        }
        constructMaps();
    }

    /**
     * Checks the merging criterion we have derived!
     * -1 if they do not overlap
     * @param oc1
     * @param oc2
     * @param beMerged
     * @return
     */
    private double shouldIMerge(ObjectCondition oc1, ObjectCondition oc2, BEExpression beMerged){
        if(!oc1.overlaps(oc2)) return -1;
        ObjectCondition intersect = oc1.intersect(oc2);
        ObjectCondition union = oc1.union(oc2);
        double lhs = intersect.computeL() / union.computeL();
        long numOfPreds = beMerged.getPolicies().stream().map(BEPolicy::getObject_conditions).mapToInt(List::size).sum();
        double rhs = (PolicyConstants.ROW_EVALUATE_COST * numOfPreds) / (PolicyConstants.IO_BLOCK_READ_COST
                + PolicyConstants.ROW_EVALUATE_COST + (PolicyConstants.ROW_EVALUATE_COST * numOfPreds) );
        return lhs - rhs;
    }

    /**
     * Estimates the cost of a guarded representation of a set of policies
     * Selectivity of guard * D * Index access + Selectivity of guard * D * cost of filter * alpha * number of predicates
     * alpha is a parameter which determines the number of predicates that are evaluated in the policy (e.g., 2/3)
     * @return
     */
    public double estimateCostOfGuardRep(ObjectCondition guard, BEExpression partition){
        long numOfPreds = partition.getPolicies().stream().map(BEPolicy::getObject_conditions).mapToInt(List::size).sum();
        return PolicyConstants.IO_BLOCK_READ_COST * PolicyConstants.NUMBER_OR_TUPLES * guard.computeL() +
                PolicyConstants.NUMBER_OR_TUPLES * guard.computeL() * PolicyConstants.ROW_EVALUATE_COST *
                        2 * numOfPreds * PolicyConstants.NUMBER_OF_PREDICATES_EVALUATED;
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

    private BEExpression mergePolicies(ObjectCondition oc1, ObjectCondition oc2){
        List<BEPolicy> beoc = oMap.get(oc1);
        List<BEPolicy> bek = oMap.get(oc2);
        BEExpression beM = new BEExpression();
        if(new HashSet<>(beoc).equals(new HashSet<>(bek))){ //identical object conditions
            beM.getPolicies().addAll(beoc);
        }
        else {
            beM.getPolicies().addAll(beoc);
            beM.getPolicies().addAll(bek);
        }
        return  beM;
    }

    /**
     *
     * TODO: Compute benefit from difference between previous and current, check PredicateExtension code
     * @param oc
     * @param lindex: -1 for elements that are not in the list
     */
    private void fillMemoized(ObjectCondition oc, int lindex){
        for (int k = lindex + 1; k < aMap.get(oc.getAttribute()).size(); k++) {
            if(k == lindex) continue;
            ObjectCondition ock = aMap.get(oc.getAttribute()).get(k);
            BEExpression beM = mergePolicies(oc, ock);
            double mBenefit = shouldIMerge(oc, ock, beM);
            memoized.put(oc.hashCode() + "." + ock.hashCode(), mBenefit);
        }
    }

    public void doYourThing() {

        for (String attribute: aMap.keySet()) {
            List<ObjectCondition> attrToOc = aMap.get(attribute);
            for (int j = 0; j < attrToOc.size(); j++) {
                fillMemoized(attrToOc.get(j), j);
            }
        }

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
            BEExpression beM = mergePolicies(m1, m2);
            //TODO: Removing the respective object conditions from aMap
            //TODO: for identical object conditions, only one instance will be removed
            //TODO: therefore oMap will include the policies that have been already merged!!!

        }
    }
}
