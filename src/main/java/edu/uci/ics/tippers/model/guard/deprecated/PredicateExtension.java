package edu.uci.ics.tippers.model.guard.deprecated;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.dbms.QueryManager;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class PredicateExtension {

    FactorSelection gExpression;
    HashMap<ObjectCondition, BEExpression> guardMap;
    QueryManager queryManager = new QueryManager();


    public PredicateExtension(FactorSelection gExpression){
        this.gExpression = gExpression;
        this.guardMap = gExpression.getGuardPartitionMapWithRemainder();
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

    /**
     * Checks the merging criterion we have derived!
     * @param oc1
     * @param oc2
     * @param beExpression
     * @return
     */
    private boolean shouldIMerge(ObjectCondition oc1, ObjectCondition oc2, BEExpression beExpression){
        if(!oc1.overlaps(oc2)) return false;
        ObjectCondition intersect = oc1.intersect(oc2);
        ObjectCondition union = oc1.union(oc2);
        double lhs = intersect.computeL() / union.computeL();
        long numOfPreds = beExpression.getPolicies().stream().map(BEPolicy::getObject_conditions).mapToInt(List::size).sum();
        double rhs = (PolicyConstants.ROW_EVALUATE_COST * numOfPreds) / (PolicyConstants.IO_BLOCK_READ_COST
                + PolicyConstants.ROW_EVALUATE_COST + (PolicyConstants.ROW_EVALUATE_COST * numOfPreds) );
        return lhs > rhs;
    }


    //TODO: Improve the overlapping condition to the criterion we have derived
    //TODO: Only union if the benefit is positive
    public void extendPredicate() {
        Map<ObjectCondition, ObjectCondition> replacementMap = new HashMap<>();
        HashMap<ObjectCondition, BEExpression> growMap = new HashMap<>(guardMap);
        for (int i = 0; i < PolicyConstants.ATTRIBUTE_INDEXES.size(); i++) {
            List<ObjectCondition> guards = getGuardsOnAttribute(PolicyConstants.ATTRIBUTE_INDEXES.get(i));
            Map<String, Double> memoized = new HashMap<>();
            for (int j = 0; j < guards.size(); j++) {
                for (int k = j + 1; k < guards.size(); k++) {
                    ObjectCondition ocj = guards.get(j);
                    ObjectCondition ock = guards.get(k);
                    BEExpression beM = growMap.get(ocj).mergeExpression(guardMap.get(ock));
                    if (!shouldIMerge(ocj, ock, beM)) continue;
                    double benefit = guardMap.get(ocj).mergeExpression(guardMap.get(ock)).estimateCostOfGuardRep(ocj.union(ock), false);
                    benefit -=(guardMap.get(ock).estimateCostOfGuardRep(ocj, false)
                            + guardMap.get(ock).estimateCostOfGuardRep(ock, false));
                    memoized.put(ocj.hashCode() + "." + ock.hashCode(), benefit);
                }
            }

            while (true) {
                if(guards.size() <= 1 || memoized.size() == 0) break;
                String maxBenefitKey = memoized.entrySet().stream().max((entry1, entry2) -> entry1.getValue()
                        > entry2.getValue() ? 1 : -1).get().getKey();
                if(memoized.get(maxBenefitKey) < 0) break; //Break condition
                ObjectCondition m1 = null;
                ObjectCondition m2 = null;
                for (ObjectCondition g: growMap.keySet()) {
                    if(!g.getAttribute().equalsIgnoreCase(PolicyConstants.ATTRIBUTE_INDEXES.get(i))) continue;
                    if(m1 != null && m2 != null) break;
                    if (g.hashCode() == Integer.parseInt(maxBenefitKey.split("\\.")[0])) m1 = g;
                    if (g.hashCode() == Integer.parseInt(maxBenefitKey.split("\\.")[1])) m2 = g;
                }
                ObjectCondition ocM = m1.union(m2);
                BEExpression beM = growMap.get(m1).mergeExpression(guardMap.get(m2));
                growMap.put(ocM, beM);
                guards.remove(m1);
                guards.remove(m2);
                guards.add(ocM);
                replacementMap.put(m1, ocM);
                replacementMap.put(m2, ocM);
                double costocM = growMap.get(ocM).estimateCostOfGuardRep(ocM, false);
                for (int j = 0; j < guards.size(); j++) {
                    ObjectCondition ocj = guards.get(j);
                    if (ocj.compareTo(ocM) == 0) continue;
                    if (!ocj.overlaps(ocM)) continue;
                    double benefit = growMap.get(ocj).estimateCostOfGuardRep(ocj, false) + costocM;
                    benefit -= growMap.get(ocM).mergeExpression(guardMap.get(ocM)).estimateCostOfGuardRep(ocj.union(ocM), false);
                    memoized.put(ocj.hashCode() + "" + ocM.hashCode(), benefit);
                }
            }
            chainEmUp(PolicyConstants.ATTRIBUTE_INDEXES.get(i), replacementMap,
                    getGuardsOnAttribute(PolicyConstants.ATTRIBUTE_INDEXES.get(i)));
        }

        //Rewriting the original expression
        Set<ObjectCondition> replacement = new HashSet<>();
        replacement.addAll(replacementMap.values());
        for(ObjectCondition rep:replacement) {
            List<ObjectCondition> replaced = new ArrayList<>(getKeysByValue(replacementMap, rep));
            BEExpression beM = guardMap.get(replaced.get((0)));
            guardMap.remove(replaced.get(0));
            for (int i = 1; i < replaced.size() ; i++) {
                beM = beM.mergeExpression(guardMap.get(replaced.get(i)));
                guardMap.remove(replaced.get(i));
            }
            guardMap.put(rep, beM);
        }
    }

    private void chainEmUp(String attribute, Map<ObjectCondition, ObjectCondition> replacementMap, List<ObjectCondition> objectConditions){
        Set<ObjectCondition> removal = new HashSet<>();
        Set<ObjectCondition> checker = replacementMap.values().stream().filter(v -> v.getAttribute()
                .equalsIgnoreCase(attribute)).collect(Collectors.toSet());
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

    public String printGuardMap(){
        StringBuilder gRep = new StringBuilder();
        String delim = "";
        for (Map.Entry<ObjectCondition, BEExpression> entry : guardMap.entrySet()) {
            gRep.append(delim);
            gRep.append(entry.getKey().print());
            gRep.append(PolicyConstants.CONJUNCTION);
            gRep.append("(");
            gRep.append(entry.getValue().createQueryFromPolices());
            gRep.append(")");
            delim = PolicyConstants.DISJUNCTION;
        }
        return gRep.toString();
    }

    public Duration computeGuardCosts(){
        long gcost = 0;
        for (Map.Entry<ObjectCondition, BEExpression> entry : guardMap.entrySet()) {
            StringBuilder query = new StringBuilder();
            query.append(entry.getKey().print());
            query.append(PolicyConstants.CONJUNCTION);
            query.append("(" +entry.getValue().createQueryFromPolices() + ")");
            long start_time = System.currentTimeMillis();
            queryManager.runTimedQuery(query.toString() ).toMillis();
            long end_time = System.currentTimeMillis();
            long difference = end_time - start_time;
            gcost += difference;
        }
        return Duration.ofMillis(gcost);
    }
}
