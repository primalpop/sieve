package edu.uci.ics.tippers.model.policy;

import java.util.ArrayList;

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
}
