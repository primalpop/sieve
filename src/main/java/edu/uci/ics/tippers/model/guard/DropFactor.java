package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.util.List;

/**
 * Created by cygnus on 11/9/17.
 */
public class DropFactor {

    private ExactFactor exactFactor;

    List<BEExpression> dropCombos;

    public void dropPredicatesFromExpression(){

        BEExpression texp = new BEExpression(exactFactor.getExpression());
        for (int i = 0; i < exactFactor.expression.getPolicies().size() ; i++) {
            BEPolicy mPolicy = exactFactor.expression.getPolicies().get(i);
            for (int j = 0; j < mPolicy.getObject_conditions().size(); j++) {
                ObjectCondition dropped = mPolicy.getObject_conditions().get(j);
                texp.getPolicies().get(i).deleteObjCond(dropped);
                for (int k = 0; k < exactFactor.expression.getPolicies().size() ; k++) {
                    if(k != i){
                        BEPolicy yPolicy = exactFactor.expression.getPolicies().get(k);
                        dropCombos.add(texp);
                        for (int l = 0; l < yPolicy.getObject_conditions().size() ; l++) {
                            ObjectCondition dropNext = mPolicy.getObject_conditions().get(l);
                            texp.getPolicies().get(k).deleteObjCond(dropNext);
                            dropCombos.add(texp);
                        }
                    }
                }
            }
        }
    }

}
