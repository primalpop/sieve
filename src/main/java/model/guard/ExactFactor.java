package model.guard;

import common.PolicyConstants;
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
public class ExactFactor extends  Factor {

    MySQLQueryManager queryManager = new MySQLQueryManager();

    ObjectCondition factor;

    List<ObjectCondition> quotient;

    List<ObjectCondition> reminder;

    Long cost;

    int false_positives;


    public void ExactFactor(){

        factor = new ObjectCondition();

        quotient = new ArrayList<ObjectCondition>();

        reminder = new ArrayList<ObjectCondition>();

        cost = 100000L;

        false_positives = 100000000;
    }

    @Override
    public void factorize(List<BEPolicy> policies, List<String> combiners) {
        ExactFactor ef = new ExactFactor();
        List<ObjectCondition> checkedObjConditions = new ArrayList<ObjectCondition>();
        Boolean factored = false;
        for (int i = 0; i < policies.size(); i++) {
            ObjectCondition objCond = policies.get(i).getObject_conditions().get();
            if (objCond.containedInList(checkedObjConditions))
                continue;
            List<ObjectCondition> factoredExp = new ArrayList<ObjectCondition>(objectConditions);
            for (int j = 0; j < objectConditions.size(); j++) {
                if (i != j) {
                    if (objCond.checkSame(objectConditions.get(j))) {
                        factoredExp.remove(objectConditions.get(j));
                        factor = objCond;
                        factored = true;
                    }
                }
            }
            if(factored){
                factoredExp.remove(factor);

            }
//            Long fCost = computeCost(factoredExp, combiners);
//            if (fCost < bfCost){
//                bfCost = fCost;
//                bf = objectConditions.get(i);
//            }
        }

    }

    /**
     * 
     * @return
     */

    public String createQuery(List<ObjectCondition> objectConditions, List<String> combiners){
        StringBuilder query = new StringBuilder();
        String delim = "";
        for (int i = 0; i < objectConditions.size(); i++) {
            ObjectCondition oc = objectConditions.get(i);
            query.append(delim);
            query.append(oc.print());
            if (i < objectConditions.size() - 1)
                delim = combiners.get(i);
        }
        System.out.println(query);
        return query.toString();
    }


    @Override
    public long computeCost(List<ObjectCondition> objectConditions, List<String> combiners){
        return queryManager.runTimedQuery(createQuery(objectConditions, combiners));
    }


    @Override
    public double computeFalsePositives(List<ObjectCondition> objectConditions, List<String> combiners) {
        return queryManager.runCountingQuery(createQuery(objectConditions, combiners));
    }
}
