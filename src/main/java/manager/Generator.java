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

        ExactFactor currentFactor = new ExactFactor(beExpression);

        List<ObjectCondition> objectConditions = beExpression.getRepeating();

        for (ObjectCondition oc : objectConditions) {
            currentFactor = new ExactFactor(beExpression);
            currentFactor.factorize(oc);
            if (bestFactor.getCost() > currentFactor.getCost()) {
                bestFactor = currentFactor;
            }
        }
        return bestFactor;
    }
}
