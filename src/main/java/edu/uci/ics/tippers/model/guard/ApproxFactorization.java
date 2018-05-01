package edu.uci.ics.tippers.model.guard;

import com.rits.cloning.Cloner;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by cygnus on 2/14/18.
 */
public class ApproxFactorization {

    Cloner cloner = new Cloner();

    BEExpression expression;

    public ApproxFactorization() {
        this.expression = new BEExpression();
    }

    public ApproxFactorization(BEExpression expression) {
        this.expression = new BEExpression(expression);
    }

    public static Calendar timestampStrToCal(String timestamp) {
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat(PolicyConstants.TIMESTAMP_FORMAT);
        try {
            cal.setTime(sdf.parse(timestamp));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return cal;
    }

    public BEExpression getExpression() {
        return expression;
    }

    public void setExpression(BEExpression expression) {
        this.expression = expression;
    }

    /**
     * Computing incremental function F of Policy of a single predicate of the form axyz to a(xyz)
     * F(ax) = -l(x) + l(ax)
     *
     * @param original
     * @param factorized
     * @return
     */
    private double computeF(BEPolicy factorized, BEPolicy original) {
        double lfact = BEPolicy.computeL(factorized.getObject_conditions());
        double lorg = BEPolicy.computeL(original.getObject_conditions());
        return lorg - lfact;
    }

    private boolean overlaps(ObjectCondition o1, ObjectCondition o2) {
        if (o1.getType().getID() == 4) { //Integer
            int start1 = Integer.parseInt(o1.getBooleanPredicates().get(0).getValue());
            int end1 = Integer.parseInt(o1.getBooleanPredicates().get(1).getValue());
            int start2 = Integer.parseInt(o2.getBooleanPredicates().get(0).getValue());
            int end2 = Integer.parseInt(o2.getBooleanPredicates().get(1).getValue());
            if (start1 <= end2 && end1 >= start2)
                return true;
        } else if (o1.getType().getID() == 2) { //Timestamp
            Long extension = (long) (60 * 1000); //1 minute extension
            Calendar start1 = timestampStrToCal(o1.getBooleanPredicates().get(0).getValue());
            Calendar start1Ext = Calendar.getInstance();
            start1Ext.setTimeInMillis(start1.getTimeInMillis() - extension);
            Calendar end1 = timestampStrToCal(o1.getBooleanPredicates().get(1).getValue());
            Calendar end1Ext = Calendar.getInstance();
            end1Ext.setTimeInMillis(end1.getTimeInMillis() + extension);
            Calendar start2 = timestampStrToCal(o2.getBooleanPredicates().get(0).getValue());
            Calendar start2Ext = Calendar.getInstance();
            start2Ext.setTimeInMillis(start2.getTimeInMillis() - extension);
            Calendar end2 = timestampStrToCal(o2.getBooleanPredicates().get(1).getValue());
            Calendar end2Ext = Calendar.getInstance();
            end2Ext.setTimeInMillis(end2.getTimeInMillis() + extension);
            if (start1Ext.compareTo(end2Ext) < 0 && end1Ext.compareTo(start2Ext) > 0) {
                return true;
            }
        } else if (o1.getType().getID() == 1) { //String
            if (o1.getAttribute().equalsIgnoreCase(PolicyConstants.USERID_ATTR)) {
                int start1 = Integer.parseInt(o1.getBooleanPredicates().get(0).getValue());
                int end1 = Integer.parseInt(o1.getBooleanPredicates().get(1).getValue());
                int start2 = Integer.parseInt(o2.getBooleanPredicates().get(0).getValue());
                int end2 = Integer.parseInt(o2.getBooleanPredicates().get(1).getValue());
                if (start1 - 1000 <= end2 + 1000 && end1 + 1000 >= start2 - 1000)
                    return true;
            } else if (o1.getAttribute().equalsIgnoreCase(PolicyConstants.LOCATIONID_ATTR)) {
                int start1 = Integer.parseInt(o1.getBooleanPredicates().get(0).getValue().substring(0, 4));
                int end1 = Integer.parseInt(o1.getBooleanPredicates().get(1).getValue().substring(0, 4));
                int start2 = Integer.parseInt(o2.getBooleanPredicates().get(0).getValue().substring(0, 4));
                int end2 = Integer.parseInt(o2.getBooleanPredicates().get(1).getValue().substring(0, 4));
                if (start1 - 100 <= end2 + 100 && end1 + 100 >= start2 - 100)
                    return true;
            } else {
                //attribute activity
                return false;
            }
        } else {
            throw new PolicyEngineException("Incompatible Attribute Type");
        }
        return false;
    }

    /**
     * returns a map with key as object condition and value as the list of policies it appears in (assuming duplicate
     * object conditions can exist).
     *
     * @param attribute
     * @return
     */
    private HashMap<ObjectCondition, List<BEPolicy>> getPredicatesOnAttr(String attribute) {
        HashMap<ObjectCondition, List<BEPolicy>> predMap = new HashMap<>();
        for (int i = 0; i < expression.getPolicies().size(); i++) {
            BEPolicy pol = expression.getPolicies().get(i);
            for (int j = 0; j < pol.getObject_conditions().size(); j++) {
                ObjectCondition oc = pol.getObject_conditions().get(j);
                if (oc.getAttribute().equalsIgnoreCase(attribute)) {
                    if (predMap.containsKey(oc)) {
                        predMap.get(oc).add(pol);
                    } else {
                        List<BEPolicy> bePolicies = new ArrayList<>();
                        bePolicies.add(pol);
                        predMap.put(oc, bePolicies);
                    }
                }
            }
        }
        return predMap;
    }

    /**
     * If the predicate that overlaps with another predicate exists in many policies, we choose the one with least
     * number of false positives to check if it can be merged.
     *
     * @param objectCondition
     * @param bePolicies
     * @return
     */
    private BEPolicy choosePolicyToMerge(ObjectCondition objectCondition, List<BEPolicy> bePolicies) {
        double minFalsePositives = PolicyConstants.INFINTIY;
        int chosen = 0;
        for (int i = 0; i < bePolicies.size(); i++) {
            BEPolicy candidate = cloner.deepClone(bePolicies.get(i));
            candidate.deleteObjCond(objectCondition);
            if (candidate.getObject_conditions().size() == 0) {
                System.out.println(bePolicies.get(i).createQueryFromObjectConditions());
            }
            double fp = BEPolicy.computeL(candidate.getObject_conditions());
            if (fp < minFalsePositives) {
                minFalsePositives = fp;
                chosen = i;
            }
        }
        return bePolicies.get(chosen);
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


    //TODO: Check if l_intersection is actually union!?! #bamboozled
    /**
     * Gain formula from Surajit's work
     * if F(a1) + F (a2) + l_intersection(a1, a2) > 0: return true, false otherwise
     *
     * @param pa1
     * @param a1
     * @param pa2
     * @param a2
     * @return
     */
    private boolean canBeMerged(BEPolicy pa1, ObjectCondition a1, BEPolicy pa2, ObjectCondition a2) {
        BEPolicy policy_a1_factorized = cloner.deepClone(pa1);
        policy_a1_factorized.deleteObjCond(a1);
        double F_a1 = computeF(policy_a1_factorized, pa1);
        BEPolicy policy_a2_factorized = cloner.deepClone(pa2);
        policy_a2_factorized.deleteObjCond(a2);
        double F_a2 = computeF(policy_a2_factorized, pa2);
        BEPolicy intersection = new BEPolicy();
        intersection.getObject_conditions().add(a1);
        intersection.getObject_conditions().add(a2);
        double l_intersection = BEPolicy.computeL(intersection.getObject_conditions());
        return (l_intersection + F_a1 + F_a2) > 0;
    }

    /**
     * Check if the two predicates can be extended by comparing if the number of tuples
     * that satisfies the extended predicate is higher than sum of the number of tuples
     * that satisfy each predicate
     * @param a1
     * @param a2
     * @return
     */
    private boolean canBeExtended(ObjectCondition a1, ObjectCondition a2) {
        if(!overlaps(a1, a2)) return false;
        BEPolicy intersection = new BEPolicy();
        intersection.getObject_conditions().add(a1);
        intersection.getObject_conditions().add(a2);
        double l_union = BEPolicy.computeL(intersection.getObject_conditions());
        if ((a1.computeL() + a2.computeL()) < l_union) return false;
        else return true;
    }

    /**
     * Generates an extended predicate based on the list of object conditions
     * @param top
     * @param overlap
     * @return
     */
    private ObjectCondition extend(ObjectCondition top, List<ObjectCondition> overlap){
        ObjectCondition extended = new ObjectCondition(top);
        String minValue = overlap.get(0).getBooleanPredicates().get(0).getValue();
        String maxValue = top.getBooleanPredicates().get(1).getValue();
        for (int k = 0; k < overlap.size(); k++) {
            if (maxValue.compareTo(overlap.get(k).getBooleanPredicates().get(1).getValue()) < 0) {
                maxValue = overlap.get(k).getBooleanPredicates().get(1).getValue();
            }
        }
        extended.getBooleanPredicates().get(0).setValue(minValue);
        extended.getBooleanPredicates().get(1).setValue(maxValue);
        extended.getBooleanPredicates().get(0).setOperator(">=");
        extended.getBooleanPredicates().get(1).setOperator("<=");
        return extended;
    }

    /**
     * Algorithm Steps
     * 1) Iterates through different attributes
     * 2) For each attribute, it builds a map of the form {predicate | [list of policies predicate appears in}
     * 3) It sorts the predicates by the begin range attribute of the predicates
     * 4) For each of the object conditions, if there's an overlap with any other predicate, it checks if the gain is
     * positive. In case the predicate appears in multiple policies, it chooses the predicate from the policy which
     * results in lowest number of false positives
     * 5) If it's positive, it merges them and stores the association between the merged and original predicates in a map
     * of the form {original | merged }
     * 6) Rewrite the original expression with the merged predicate. In case, the same predicate appears in multiple
     * policies all of them are merged.
     */
    public void approximateFactorization() {
        Map<ObjectCondition, ObjectCondition> replacementMap = new HashMap<>();
        for (int i = 0; i < this.expression.getPolAttributes().size(); i++) {
            HashMap<ObjectCondition, List<BEPolicy>> predOnAttr = getPredicatesOnAttr(this.expression.getPolAttributes().get(i));
            if (predOnAttr.isEmpty()) continue;
            List<ObjectCondition> objectConditions = new ArrayList<>();
            objectConditions.addAll(predOnAttr.keySet());
            Collections.sort(objectConditions);
            Stack<ObjectCondition> stack = new Stack<>();
            stack.push(objectConditions.get(0));
            List<ObjectCondition> overlap = new ArrayList<>();
            int j = 0;
            do {
                j++;
                ObjectCondition top = stack.peek();
                if(j == objectConditions.size() && !overlap.isEmpty()){
                    ObjectCondition extended = extend(top, overlap);
                    replacementMap.put(stack.pop(), extended); //overlap doesn't include the top element
                    for (int k = 0; k < overlap.size(); k++) {
                        replacementMap.put(stack.pop(), extended);
                    }
                    break;
                }
                if(j == objectConditions.size()) break;
                ObjectCondition next = objectConditions.get(j);
                Boolean extend = canBeExtended(top, next);
                if (!extend) { //cannot be extended
                    if (overlap.isEmpty()) stack.push(next); //no previous elements that overlap
                    else { //extending the previous elements that overlap
                        ObjectCondition extended = extend(top, overlap);
                        replacementMap.put(stack.pop(), extended); //overlap doesn't include the top element
                        for (int k = 0; k < overlap.size(); k++) {
                            replacementMap.put(stack.pop(), extended);
                        }
                        stack.push(extended);
                        overlap = new ArrayList<>();
                        j--;
                    }
                }
                else { //can be extended
                    overlap.add(top);
                    stack.push(next);
                }
            }
            while ( j < objectConditions.size());
            //Clean the replacement where chains exist
            chainEmUp(replacementMap, objectConditions);
        }
        //Rewriting the original expression
        for(ObjectCondition pred:replacementMap.keySet()) {
            this.expression.replenishFromPolicies(pred, replacementMap.get(pred));
        }
    }

    private Map<ObjectCondition, ObjectCondition> chainEmUp(Map<ObjectCondition, ObjectCondition> replacementMap, List<ObjectCondition> objectConditions){
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
        return replacementMap;
    }
}
