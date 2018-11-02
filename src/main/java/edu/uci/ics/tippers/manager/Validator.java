package edu.uci.ics.tippers.manager;
import edu.uci.ics.tippers.model.policy.BEPolicy;

/**
 * At the time of inputting a policy P, the policy management module validates it by checking
 * if the data subject in the policy is a valid user and if the policy author is allowed to author that policy
 * (i.e., they are the data subject itself or has been authorized by the data subject).
 */
public class Validator {

    private static Validator _instance = new Validator();

    public static Validator getInstance() {
        return _instance;
    }

    private Boolean validate(BEPolicy bePolicy){
        return true;
    }
}
