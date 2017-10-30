package model.guard;

import model.policy.BooleanCondition;
import model.policy.ObjectCondition;

import java.util.List;

/**
 * Created by cygnus on 10/25/17.
 */
public abstract class Factor {
    
    BooleanCondition factor;

    List<BooleanCondition> Quotient;

    List<BooleanCondition> reminder;

    public abstract void factorize();

    public abstract long computeCost(List<ObjectCondition> objectConditions);

    public abstract double computeFalsePositives();

}
