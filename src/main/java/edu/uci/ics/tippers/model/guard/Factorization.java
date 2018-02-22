package edu.uci.ics.tippers.model.guard;

import com.rits.cloning.Cloner;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by cygnus on 2/14/18.
 */
public class Factorization {

    MySQLQueryManager queryManager = new MySQLQueryManager();

    Cloner cloner = new Cloner();

    BEExpression expression;

    public BEExpression getExpression() {
        return expression;
    }

    public void setExpression(BEExpression expression) {
        this.expression = expression;
    }

    public Factorization(){
        this.expression = new BEExpression();
    }

    public Factorization(BEExpression expression){
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

    private long computeL(BEPolicy bePolicy){
        String query = PolicyConstants.SELECT_COUNT_STAR_SEMANTIC_OBSERVATIONS + " where " +  bePolicy.createQueryFromObjectConditions();
        return queryManager.runCountingQuery(query);
    }

    private long computeL(ObjectCondition objectCondition){
        String query = PolicyConstants.SELECT_COUNT_STAR_SEMANTIC_OBSERVATIONS + " where " +  objectCondition.print();
        return queryManager.runCountingQuery(query);

    }

    /**
     * Computing incremental function F of Policy of a single predicate of the form axyz to a(xyz)
     * F(ax) = -l(x) + l(ax)
     * @param original
     * @param factorized
     * @return
     */
    private long computeF(BEPolicy factorized, BEPolicy original){
        long lfact = computeL(factorized);
        long lorg = computeL(original);
        return lorg - lfact;
    }

    private boolean overlaps(ObjectCondition o1, ObjectCondition o2){
        if(o1.getType().getID() == 4){ //Integer
            int start1 = Integer.parseInt(o1.getBooleanPredicates().get(0).getValue());
            int end1 = Integer.parseInt(o1.getBooleanPredicates().get(1).getValue());
            int start2 = Integer.parseInt(o2.getBooleanPredicates().get(0).getValue());
            int end2 = Integer.parseInt(o2.getBooleanPredicates().get(1).getValue());
            if (start1 <= end2 && end1 >= start2)
                return true;
        }
        else if(o1.getType().getID() == 2) { //Timestamp
            Calendar start1 = timestampStrToCal(o1.getBooleanPredicates().get(0).getValue());
            Calendar end1 = timestampStrToCal(o1.getBooleanPredicates().get(1).getValue());
            Calendar start2 = timestampStrToCal(o2.getBooleanPredicates().get(0).getValue());
            Calendar end2 = timestampStrToCal(o2.getBooleanPredicates().get(1).getValue());
            if (start1.compareTo(end2) < 0 && end1.compareTo(start2) > 0) {
                return true;
            }
        }
        else if (o1.getType().getID() == 1){
            String start1 = o1.getBooleanPredicates().get(0).getValue();
            String end1 = o1.getBooleanPredicates().get(1).getValue();
            String start2 = o2.getBooleanPredicates().get(0).getValue();
            String end2 = o2.getBooleanPredicates().get(1).getValue();
            if(start1.compareTo(end2) < 0 && end1.compareTo(start2) > 0){
                return true;
            }
        }
        else{
            throw new PolicyEngineException("Incompatible Attribute Type");
        }
        return false;
    }

    /**
     * returns a map with key as object condition and value as the list of policies it appears in (assuming duplicate
     * object conditions can exist).
     * @param attribute
     * @return
     */
    private HashMap<ObjectCondition, List<BEPolicy>> getPredicatesOnAttr(String attribute){
        HashMap<ObjectCondition, List<BEPolicy>> predMap = new HashMap<>();
        for (int i = 0; i < expression.getPolicies().size(); i++) {
            BEPolicy pol = expression.getPolicies().get(i);
            for (int j = 0; j < pol.getObject_conditions().size(); j++) {
                ObjectCondition oc = pol.getObject_conditions().get(j);
                if(oc.getAttribute().equalsIgnoreCase(attribute)){
                    if(predMap.containsKey(oc)){
                        predMap.get(oc).add(pol);
                    }
                    else{
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
     * @param objectCondition
     * @param bePolicies
     * @return
     */
    private BEPolicy choosePolicyToMerge(ObjectCondition objectCondition, List<BEPolicy> bePolicies){
        long minFalsePositives = PolicyConstants.INFINTIY;
        int chosen = 0;
        for (int i = 0; i < bePolicies.size(); i++) {
            BEPolicy candidate = cloner.deepClone(bePolicies.get(i));
            candidate.deleteObjCond(objectCondition);
            long fp = computeL(candidate);
            if(fp < minFalsePositives ){
                minFalsePositives = fp;
                chosen = i;
            }
        }
        return bePolicies.get(chosen);
    }

    private boolean canBeMerged(BEPolicy pa1, ObjectCondition a1, BEPolicy pa2, ObjectCondition a2){
        BEPolicy policy_a1_factorized = cloner.deepClone(pa1);
        policy_a1_factorized.deleteObjCond(a1);
        long F_a1 = computeF(policy_a1_factorized, pa1);
        BEPolicy policy_a2_factorized = cloner.deepClone(pa2);
        policy_a2_factorized.deleteObjCond(a2);
        long F_a2 = computeF(policy_a2_factorized, pa2);
        BEPolicy intersection = new BEPolicy();
        intersection.getObject_conditions().add(a1);
        intersection.getObject_conditions().add(a2);
        long l_intersection = computeL(intersection);
        return (l_intersection + F_a1 + F_a2) > 0;
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
    public void approximateFactorization(){
        Map<ObjectCondition, ObjectCondition> replacementMap = new HashMap<>();
        for (int i = 0; i < this.expression.getPolAttributes().size(); i++) {
            HashMap<ObjectCondition, List<BEPolicy>> predOnAttr = getPredicatesOnAttr(this.expression.getPolAttributes().get(i));
            if(predOnAttr.isEmpty())
                continue;
            List<ObjectCondition> objectConditions = new ArrayList<>();
            objectConditions.addAll(predOnAttr.keySet());
            Collections.sort(objectConditions);
            Stack<ObjectCondition> stack = new Stack<>();
            stack.push(objectConditions.get(0));
            for (int j = 1; j < objectConditions.size(); j++) {
                ObjectCondition top = stack.peek();
                ObjectCondition next = objectConditions.get(j);
                if(!overlaps(top, next))
                    stack.push(next);
                else {
                    List<BEPolicy> policy_a1_list = predOnAttr.get(next);
                    List<BEPolicy> policy_a2_list = predOnAttr.get(top);
                    BEPolicy policy_a1 = new BEPolicy(choosePolicyToMerge(next, policy_a1_list));
                    BEPolicy policy_a2 = new BEPolicy(choosePolicyToMerge(top, policy_a2_list));
                    if(canBeMerged(policy_a1, next, policy_a2, top)){
                        if(next.getBooleanPredicates().get(1).getValue().compareTo(top.getBooleanPredicates().get(1).getValue()) > 0){
                            top.getBooleanPredicates().get(1).setValue(next.getBooleanPredicates().get(1).getValue());
                        }
                        replacementMap.put(stack.pop(), top);
                        replacementMap.put(next, top);
                        stack.push(top);
                    }
                }
            }
            //Rewriting the original expression
            for (ObjectCondition pred: replacementMap.keySet()) {
                this.expression.replaceFromPolicies(pred, replacementMap.get(pred));
            }
        }
    }
}
