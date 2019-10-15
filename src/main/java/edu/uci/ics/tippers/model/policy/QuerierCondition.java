package edu.uci.ics.tippers.model.policy;

import edu.uci.ics.tippers.common.AttributeType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by cygnus on 9/25/17.
 */
public class QuerierCondition extends BooleanCondition {

    public QuerierCondition(){

    }

    public QuerierCondition(QuerierCondition qc){
        this.attribute = qc.getAttribute();
        this.type = qc.getType();
        this.booleanPredicates = new ArrayList<BooleanPredicate>(qc.getBooleanPredicates().size());
        for(BooleanPredicate bp: qc.getBooleanPredicates()){
            this.booleanPredicates.add(new BooleanPredicate(bp));
        }
    }

    public QuerierCondition(String policyID, String attribute, AttributeType attributeType, Operation o1, String v1){
        this.policy_id = policyID;
        this.attribute = attribute;
        this.type = attributeType;
        List<BooleanPredicate> booleanPredicates = new ArrayList<>();
        booleanPredicates.add(new BooleanPredicate(o1, v1));
        this.booleanPredicates = booleanPredicates;
    }

    /**
     * Returns true if user policy, false otherwise
     * @return
     */
    public boolean checkTypeOfPolicy(){
        return this.booleanPredicates.get(0).getValue().equalsIgnoreCase("user");
    }

}
