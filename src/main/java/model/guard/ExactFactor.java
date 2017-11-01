package model.guard;

import common.PolicyConstants;
import common.PolicyEngineException;
import db.MySQLQueryManager;
import model.policy.BEPolicy;
import model.policy.BooleanCondition;
import model.policy.BooleanPredicate;
import model.policy.ObjectCondition;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by cygnus on 10/29/17.
 *
 * E = D.Q + R
 * where D is an exact factor
 */
public class ExactFactor{

    MySQLQueryManager queryManager = new MySQLQueryManager();

    //Original expression
    List<BEPolicy> expression;

    // Chosen factor
    ObjectCondition factor;

    // Polices from the original expression that contain the factor
    List<BEPolicy> quotient;

    // Polices from the original expression that does not contain the factor
    List<BEPolicy> reminder;

    //Cost of evaluating the expression
    Long cost;

    public void ExactFactor(List<BEPolicy> policies){
        expression = new ArrayList<BEPolicy>(policies);
        factor = new ObjectCondition();
        quotient = new ArrayList<BEPolicy>();
        reminder = new ArrayList<BEPolicy>();
        cost = PolicyConstants.INFINTIY;
    }

    /**
     * Given a list of policies identify the unique set of object conditions
     * Currently only identifies a single object condition as factor <-- to be extended to multiple object condition
     * @param policies
     * @return
     */
    public List<ObjectCondition> getUnique(List<BEPolicy> policies){
        List<ObjectCondition> objConds = new ArrayList<ObjectCondition>();
        for (int i = 0; i < policies.size(); i++) {
            List<ObjectCondition> policyCond = policies.get(i).getObject_conditions();
            for (int j = 0; j < policyCond.size(); j ++){
                if(!policyCond.get(j).containedInList(objConds)){
                    objConds.add(policyCond.get(j));
                }
            }
        }
        return objConds;
    }

    /**
     * Given a predicate and list of policies, identify the list of policies containing that predicate
     * @param objectCondition
     * @param policies
     * @return
     */
    public List<BEPolicy> checkAgainstPolicies(ObjectCondition objectCondition, List<BEPolicy> policies){
        List<BEPolicy> polConPred = new ArrayList<BEPolicy>();
        for (int i = 0; i < policies.size(); i++) {
            if(policies.get(i).containsObjCond(objectCondition))
                polConPred.add(policies.get(i));
        }
        return polConPred;
    }

    /**
     * Deletes the given predicate from the policies that contain it
     * @param objectCondition
     * @param policies
     * @return
     */
    public List<BEPolicy> removeFromPolicies(ObjectCondition objectCondition, List<BEPolicy> policies){
        List<BEPolicy> polRemPred = new ArrayList<BEPolicy>(policies);
        for (int i = 0; i < policies.size(); i++) {
            polRemPred.get(i).deleteObjCond(objectCondition);
        }
        return polRemPred;
    }

    /**
     * Substract a list of policies from another
     * @param policies
     * @param expression
     * @return
     */
    public List<BEPolicy> deletePolicies(List<BEPolicy> expression, List<BEPolicy> policies){
        for (BEPolicy exp: expression) {
            for (BEPolicy p: policies) {
                if (p.compareTo(exp) == 0){
                    expression.remove(exp);
                }
            }
        }
        return expression;
    }

    public void factorize(List<BEPolicy> policies){
        List<ObjectCondition> objectConditions = getUnique(policies);
        ExactFactor ef = new ExactFactor();
        ef.expression = policies;
        for (ObjectCondition oc: objectConditions) {
            List<BEPolicy> q = checkAgainstPolicies(oc, ef.expression);
            if(q.size() > 1){ //was able to factor
                ef.factor = oc;
                ef.quotient = q;
                ef.reminder = deletePolicies(ef.expression, ef.quotient);
                ef.cost = queryManager.runTimedQuery(ef.createQueryFromExactFactor());
            }
        }
    }

    //TODO: Start here tomorrow

    /**
     * Creates a query string from Exact Factor by
     * AND'ing factor and quotient and by OR'ing the reminder
     * @return
     */
    public String createQueryFromExactFactor(){
        return null;
    }



    /**
     * Given a list of policies, it creates a query by
     * AND'ing object conditions that belong to the same policy
     * and by OR'ing the object conditions across policies
     * @param policies
     * @return
     */
    public String createQueryFromPolices(List<BEPolicy> policies){
        StringBuilder query = new StringBuilder();
        String delim = "";
        for (int i = 0; i < policies.size(); i++) {
            query.append(delim);
            query.append(policies.get(i).createQueryFromObjectConditions());
            delim = PolicyConstants.DISJUNCTION;
        }
        return query.toString();
    }

    public long computeCost(List<BEPolicy> policies){
        return queryManager.runTimedQuery(createQueryFromPolices(policies));
    }


    public double computeFalsePositives(List<BEPolicy> policies) {
        return queryManager.runCountingQuery(createQueryFromPolices(policies));
    }
}



//
//    public void factorize(List<BEPolicy> policies) {
//        ExactFactor ef = new ExactFactor();
//
//        List<ObjectCondition> checkedObjConditions = new ArrayList<ObjectCondition>();
//        Boolean factored = false;
//        for (int i = 0; i < policies.size(); i++) {
//            ObjectCondition objCond = policies.get(i).getObject_conditions().get();
//            if (objCond.containedInList(checkedObjConditions))
//                continue;
//            List<ObjectCondition> factoredExp = new ArrayList<ObjectCondition>(objectConditions);
//            for (int j = 0; j < objectConditions.size(); j++) {
//                if (i != j) {
//                    if (objCond.checkSame(objectConditions.get(j))) {
//                        factoredExp.remove(objectConditions.get(j));
//                        factor = objCond;
//                        factored = true;
//                    }
//                }
//            }
//            if(factored){
//                factoredExp.remove(factor);
//
//            }
////            Long fCost = computeCost(factoredExp, combiners);
////            if (fCost < bfCost){
////                bfCost = fCost;
////                bf = objectConditions.get(i);
////            }
//        }
//
//    }
