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

    public QuerierCondition(String attribute, AttributeType attributeType, String o1, String v1, String o2, String v2){
        this.attribute = attribute;
        this.type = attributeType;
        List<BooleanPredicate> booleanPredicates = new ArrayList<>();
        booleanPredicates.add(new BooleanPredicate(o1, v1));
        booleanPredicates.add(new BooleanPredicate(o2, v2));
        this.booleanPredicates = booleanPredicates;
    }

}
