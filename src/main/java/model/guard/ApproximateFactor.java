package model.guard;

import model.policy.ObjectCondition;

import java.util.List;

/**
 * Created by cygnus on 10/29/17.
 */
public class ApproximateFactor extends Factor {

    @Override
    public void factorize() {

    }

    @Override
    public long computeCost(List<ObjectCondition> objectConditions) {
        return 0;
    }
    
    @Override
    public double computeFalsePositives() {
        return 0;
    }
}
