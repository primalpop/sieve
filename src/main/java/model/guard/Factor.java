package model.guard;

import model.policy.BEPolicy;
import model.policy.BooleanCondition;
import model.policy.ObjectCondition;

import java.util.List;

/**
 * Created by cygnus on 10/25/17.
 */
public abstract class Factor {
    
    BooleanCondition factor;

    List<BooleanCondition> quotient;

    List<BooleanCondition> reminder;

    public abstract void factorize(List<BEPolicy> policies, List<String> combiners);

    public abstract long computeCost(List<ObjectCondition> objectConditions, List<String> combiners);

    public abstract double computeFalsePositives(List<ObjectCondition> objectConditions, List<String> combiners);

}
