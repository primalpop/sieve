package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class FactorSelection {

    //Original expression
    BEExpression expression;

    // Chosen multiple
    List<ObjectCondition> multiplier;

    // Polices from the original expression that contain the multiple
    FactorSelection quotient;

    // Polices from the original expression that does not contain the multiple
    FactorSelection remainder;

    //Cost of evaluating the expression
    double cost;

    public FactorSelection(BEExpression expression) {
        this.expression = new BEExpression(expression);
        this.multiplier = new ArrayList<ObjectCondition>();
    }

    public BEExpression getExpression() {
        return expression;
    }

    public void setExpression(BEExpression expression) {
        this.expression = expression;
    }

    public List<ObjectCondition> getMultiplier() {
        return multiplier;
    }

    public void setMultiplier(List<ObjectCondition> multiplier) {
        this.multiplier = multiplier;
    }

    public FactorSelection getQuotient() {
        return quotient;
    }

    public void setQuotient(FactorSelection quotient) {
        this.quotient = quotient;
    }

    public FactorSelection getRemainder() {
        return remainder;
    }

    public void setRemainder(FactorSelection remainder) {
        this.remainder = remainder;
    }

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }

    /**
     * Factorization based on a single object condition and not all the possible combinations
     * After selecting factor, they are not removed from the quotient
     */
    public void selectFactor() {
        Boolean factorized = false;
        Set<ObjectCondition> singletonSet = this.expression.getPolicies().stream()
                .flatMap(p -> p.getObject_conditions().stream())
                .filter(o -> PolicyConstants.INDEXED_ATTRS.contains(o.getAttribute()))
                .collect(Collectors.toSet());
        FactorSelection currentFactor = new FactorSelection(this.expression);
        for (ObjectCondition objectCondition : singletonSet) {
            BEExpression temp = new BEExpression(this.expression);
            temp.checkAgainstPolices(objectCondition);
            if (temp.getPolicies().size() > 1) { //was able to factorize
                currentFactor.multiplier = new ArrayList<>();
                currentFactor.multiplier.add(objectCondition);
                currentFactor.remainder = new FactorSelection(this.expression);
                currentFactor.remainder.expression.getPolicies().removeAll(temp.getPolicies());
                currentFactor.quotient = new FactorSelection(temp);
//                currentFactor.quotient.expression.removeFromPolicies(objectCondition);
                currentFactor.cost = estimateCostOfGuardRep(objectCondition, currentFactor.quotient.getExpression());
                if (temp.estimateCost() > currentFactor.cost) {
                    this.multiplier = currentFactor.getMultiplier();
                    this.quotient = currentFactor.getQuotient();
                    this.remainder = currentFactor.getRemainder();
                    this.cost = currentFactor.cost;
                    factorized = true;
                }
                currentFactor = new FactorSelection(this.expression);
            }
        }
        if(!factorized) return;
        if((this.remainder.getExpression().getPolicies().size() <= 1 )) return;
        this.remainder.selectFactor();
    }


    public String createQueryFromExactFactor() {
        if (multiplier.isEmpty()) {
            if (expression != null) {
                return this.expression.createQueryFromPolices();
            } else
                return "";
        }
        StringBuilder query = new StringBuilder();
        for (ObjectCondition mul : multiplier) {
            query.append(mul.print());
            query.append(PolicyConstants.CONJUNCTION);
        }
        query.append("(");
        query.append(this.quotient.createQueryFromExactFactor());
        query.append(")");
        if (!this.remainder.expression.getPolicies().isEmpty()) {
            query.append(PolicyConstants.DISJUNCTION);
            query.append("(");
            query.append(this.remainder.createQueryFromExactFactor());
            query.append(")");
        }
        return query.toString();
    }

    /**
     * Estimates the cost of a guarded representation of a set of policies
     * Selectivity of guard * D * Index access + Selectivity of guard * D * cost of filter * alpha * number of predicates
     * alpha is a parameter which determines the number of predicates that are evaluated in the policy (e.g., 2/3)
     * @return
     */
    public double estimateCostOfGuardRep(ObjectCondition guard, BEExpression partition){
        long numOfPreds = partition.getPolicies().stream().collect(Collectors.counting());
        return PolicyConstants.IO_BLOCK_READ_COST * PolicyConstants.NUMBER_OR_TUPLES * guard.computeL() +
                PolicyConstants.NUMBER_OR_TUPLES * guard.computeL() * PolicyConstants.ROW_EVALUATE_COST *
                        2 * numOfPreds * PolicyConstants.NUMBER_OF_PREDICATES_EVALUATED;
    }


    /**
     * returns a map with key as guards and value as the guarded representation of the partition of policies
     * guard is a single object condition
     * @return
     */
    public HashMap<ObjectCondition, BEExpression> getGuardPartitionMap() {
        if(this.getMultiplier().isEmpty()){
            HashMap<ObjectCondition, BEExpression> remainderMap = new HashMap<>();
            for(BEPolicy bePolicy: this.expression.getPolicies()){
                double freq = PolicyConstants.NUMBER_OR_TUPLES;
                ObjectCondition gOC = new ObjectCondition();
                for (ObjectCondition oc: bePolicy.getObject_conditions()) {
                    if(!PolicyConstants.INDEXED_ATTRS.contains(oc.getAttribute())) continue;
                    if (oc.computeL() < freq) {
                        freq = oc.computeL();
                        gOC = oc;
                    }
                }
                BEExpression quo = new BEExpression();
                quo.getPolicies().add(bePolicy);
                remainderMap.put(gOC, quo);
            }
            return remainderMap;
        }
        HashMap<ObjectCondition, BEExpression> gMap = new HashMap<>();
        gMap.put(this.getMultiplier().get(0), this.getQuotient().getExpression());
        if(!this.getRemainder().expression.getPolicies().isEmpty()){
            gMap.putAll(this.getRemainder().getGuardPartitionMap());
        }
        return gMap;
    }
}
