package edu.uci.ics.tippers.model.policy;

import edu.uci.ics.tippers.common.AttributeType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by cygnus on 9/25/17.
 */
public class ObjectCondition extends BooleanCondition {

    public ObjectCondition() {

    }

    public ObjectCondition(ObjectCondition oc){
        this.attribute = oc.getAttribute();
        this.type = oc.getType();
        this.booleanPredicates = new ArrayList<BooleanPredicate>(oc.getBooleanPredicates().size());
        for(BooleanPredicate bp: oc.getBooleanPredicates()){
            this.booleanPredicates.add(new BooleanPredicate(bp));
        }
    }

    public ObjectCondition(String attribute, AttributeType attributeType, String o1, String v1, String o2, String v2){
        this.attribute = attribute;
        this.type = attributeType;
        List<BooleanPredicate> booleanPredicates = new ArrayList<>();
        booleanPredicates.add(new BooleanPredicate(o1, v1));
        booleanPredicates.add(new BooleanPredicate(o2, v2));
    }
}
