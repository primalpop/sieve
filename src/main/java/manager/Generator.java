package manager;

import model.guard.ExactFactor;
import model.policy.BEExpression;
import model.policy.ObjectCondition;

import java.util.List;

/**
 * Created by cygnus on 7/7/17.
 */
public class Generator {


//    public ExactFactor generateGuard(BEExpression beExpression){
//
//        ExactFactor bestFactor = new ExactFactor();
//
//        List<ObjectCondition> objectConditions = beExpression.getRepeating();
//
//        for (ObjectCondition oc: objectConditions) {
//            bestFactor.factorize(oc);
//            if(bestFactor.getCost() < )
//
//                BEExpression q = new BEExpression();
//                q.setPolicies(this.expression.checkAgainstPolicies(oc));
//                if(q.getPolicies().size() > 1){ //was able to factor
//                    this.factor = oc;
//                    this.quotient.setPolicies(q.removeFromPolicies(oc));
//                    this.reminder = expression;
//                    this.reminder.getPolicies().removeAll(q.getPolicies());
//                    this.cost = queryManager.runTimedQuery(this.createQueryFromExactFactor());
//                }
//            }
//        }
//
//    }

}
