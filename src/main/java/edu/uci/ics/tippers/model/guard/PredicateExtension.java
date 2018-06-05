package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.util.*;

public class PredicateExtension {

    FactorSelection gExpression;
    HashMap<ObjectCondition, BEExpression> guardMap;

    public PredicateExtension(FactorSelection gExpression){
        this.gExpression = gExpression;
        this.guardMap = gExpression.getGuardPartitionMap();
    }

    /**
     * ref: https://stackoverflow.com/questions/1383797/java-hashmap-how-to-get-key-from-value
     * @param replacementMap
     * @param value
     * @return
     */
    public Set<ObjectCondition> getKeysByValue(Map<ObjectCondition, ObjectCondition> replacementMap, ObjectCondition value) {
        Set<ObjectCondition> keys = new HashSet<ObjectCondition>();
        for (Map.Entry<ObjectCondition, ObjectCondition> entry : replacementMap.entrySet()) {
            if (entry.getValue().equals(value)) {
                keys.add(entry.getKey());
            }
        }
        return keys;
    }

    private List<ObjectCondition> getGuardsOnAttribute(String attribute) {
        List<ObjectCondition> gOnA = new ArrayList<>();
        for (ObjectCondition oc: guardMap.keySet()) {
            if(oc.getAttribute().equalsIgnoreCase(attribute)) gOnA.add(oc);
        }
        return gOnA;
    }



    //TODO: replacement map for predicates to be replaced by merged predicate
    //TODO: chaining them up like in the previous implementation
    //TODO: Improve the overlapping condition to the criterion we have derived
    public void extendPredicate() {
        Map<ObjectCondition, ObjectCondition> replacementMap = new HashMap<>();
        for (int i = 0; i < PolicyConstants.INDEXED_ATTRS.size(); i++) {
            List<ObjectCondition> guards = getGuardsOnAttribute(PolicyConstants.INDEXED_ATTRS.get(i));
            Map<String, Double> memoized = new HashMap<>();
            for (int j = 0; j < guards.size(); j++) {
                for (int k = j + 1; k < guards.size(); k++) {
                    ObjectCondition ocj = guards.get(j);
                    ObjectCondition ock = guards.get(k);
                    if (!ocj.overlaps(ock)) continue;
                    double benefit = (gExpression.estimateCostOfGuardRep(ocj, guardMap.get(ocj))
                            + gExpression.estimateCostOfGuardRep(ock, guardMap.get(ock)));
                    benefit -= gExpression.estimateCostOfGuardRep(ocj.merge(ock), guardMap.get(ocj).mergeExpression(guardMap.get(ock)));
                    memoized.put(ocj.hashCode() + "" + ock.hashCode(), benefit);
                }
            }

            while (true) {
                if(guards.size() <= 1) break;
                String maxBenefitKey = memoized.entrySet().stream().max((entry1, entry2) -> entry1.getValue()
                        > entry2.getValue() ? 1 : -1).get().getKey();
                if(memoized.get(maxBenefitKey) < 0) break; //Break condition
                ObjectCondition m1 = new ObjectCondition();
                ObjectCondition m2 = new ObjectCondition();
                for (int j = 0; j < guards.size(); j++) {
                    if (guards.get(j).hashCode() == Integer.parseInt(maxBenefitKey.substring(0, 1))) m1 = guards.get(j);
                    if (guards.get(j).hashCode() == Integer.parseInt(maxBenefitKey.substring(1, 2))) m2 = guards.get(j);
                }
                ObjectCondition ocM = m1.merge(m2);
                BEExpression beM = guardMap.get(m1).mergeExpression(guardMap.get(m2));
                guardMap.put(ocM, beM);
                guards.remove(m1);
                guards.remove(m2);
                guards.add(ocM);
                replacementMap.put(m1, ocM);
                replacementMap.put(m2, ocM);
                double costocM = gExpression.estimateCostOfGuardRep(ocM, guardMap.get(ocM));
                for (int j = 0; j < guards.size(); j++) {
                    ObjectCondition ocj = guards.get(j);
                    if (ocj.compareTo(ocM) == 0) continue;
                    if (!ocj.overlaps(ocM)) continue;
                    double benefit = gExpression.estimateCostOfGuardRep(ocj, guardMap.get(ocj)) + costocM;
                    benefit -= gExpression.estimateCostOfGuardRep(ocj.merge(ocM), guardMap.get(ocM).mergeExpression(guardMap.get(ocM)));
                    memoized.put(ocj.hashCode() + "" + ocM.hashCode(), benefit);
                }
            }
        }

        //Rewriting the original expression
//        for(ObjectCondition pred:replacementMap.keySet()) {
//            this.expression.replenishFromPolicies(pred, replacementMap.get(pred));
//        }
//

    }



    private void chainEmUp(Map<ObjectCondition, ObjectCondition> replacementMap, List<ObjectCondition> objectConditions){
        Set<ObjectCondition> removal = new HashSet<>();
        Set<ObjectCondition> checker = new HashSet<>(replacementMap.values());
        for (ObjectCondition ext: checker) {
            if (replacementMap.containsKey(ext)){
                if(!(objectConditions.contains(ext))) removal.add(ext);
                Set<ObjectCondition> matches = getKeysByValue(replacementMap, ext);
                for (ObjectCondition mKey: matches) {
                    replacementMap.put(mKey, replacementMap.get(ext));
                }
            }
        }
        replacementMap.keySet().removeAll(removal);
    }


}
