package model.guard;

import model.policy.BooleanCondition;

import java.util.List;

/**
 * Created by cygnus on 10/25/17.
 */
public abstract class Factor {

    BooleanCondition factor;

    List<BooleanCondition> Quotient;

    List<BooleanCondition> reminder;

    public abstract void factorize();

    public abstract double computeCost();

    public abstract double computeFalsePositives();

}
