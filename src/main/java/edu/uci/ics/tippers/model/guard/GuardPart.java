package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

public class GuardPart {

    String id;

    ObjectCondition guard;

    BEExpression guardPartition;

    double cardinality;

    /**
     * if inline is true, then policies in guardPartition are inlined for evaluation
     * else, udf is used for evaluating the policies
     */
    boolean inline;

    public ObjectCondition getGuard() {
        return guard;
    }

    public void setGuard(ObjectCondition guard) {
        this.guard = guard;
    }

    public BEExpression getGuardPartition() {
        return guardPartition;
    }

    public void setGuardPartition(BEExpression guardPartition) {
        this.guardPartition = guardPartition;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getCardinality() {
        return cardinality;
    }

    public void setCardinality(double cardinality) {
        this.cardinality = cardinality;
    }

    public boolean isInline() {
        return inline;
    }

    public void setInline(boolean inline) {
        this.inline = inline;
    }

    /**
     * cost = size(D) * sel(g) * number of policies * alpha (2/3) * policy_eval_cost
     * TODO: Replace PolicyConstants.NUMBER_OF_PREDICATES_EVALUATED (alpha) with numbers from policy efficacy check
     */
    public double estimateCostOfInline(){
        return PolicyConstants.getNumberOfTuples() * guard.computeL()
                * guardPartition.getPolicies().size() * PolicyConstants.NUMBER_OF_PREDICATES_EVALUATED
                * PolicyConstants.POLICY_EVAL_COST;
    }

    /**
     * cost = size(D) * sel(g) * (udf_invocation_cost * policy_eval_cost)
     */
    public double estimateCostOfUDF(){
        return  PolicyConstants.getNumberOfTuples() * guard.computeL() * PolicyConstants.UDF_INVOCATION_COST;
    }
}
