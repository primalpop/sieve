package manager;

import model.guard.ExactFactor;
import model.policy.BEExpression;
import model.policy.BEPolicy;
import model.policy.ObjectCondition;

import java.util.List;

/**
 * Created by cygnus on 7/7/17.
 */
public class Generator {


    public ExactFactor generateGuard(BEExpression beExpression) {

        ExactFactor bestFactor = new ExactFactor();

        ExactFactor currentFactor = new ExactFactor();

        //Error because current Factor expression doesn't get reset to the original policy

        List<ObjectCondition> objectConditions = beExpression.getRepeating();

        for (ObjectCondition oc : objectConditions) {
            currentFactor.setExpression(beExpression);
            currentFactor.factorize(oc);
            if (bestFactor.getCost() > currentFactor.getCost()) {
                bestFactor = new ExactFactor(currentFactor);
            }
        }
        return bestFactor;
    }
}
