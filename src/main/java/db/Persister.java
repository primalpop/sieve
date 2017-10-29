package db;

import model.policy.BEPolicy;
import model.policy.BooleanCondition;

/**
 * Created by cygnus on 9/25/17.
 */
public interface Persister {

    //Create persists the policy in database
    public void create(BEPolicy policy);

    //Get retrieves a policy
    public BEPolicy get(String ID);

    //Delete removes a policy
    public void delete(String ID);

    //Find all policies associated with the Querier
    public BEPolicy[] FindPoliciesForQuerier(BooleanCondition querier);

    public BEPolicy[] FindPoliciesforObject(BooleanCondition object);
}
