package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class FactorSelection {

    //Original expression
    BEExpression expression;

    // Chosen factor/guard
    //TODO: Support multiple object conditions as factor
    List<ObjectCondition> multiplier;

    // Polices from the original expression that contain the factor
    FactorSelection quotient;

    // Polices from the original expression that does not contain the factor
    FactorSelection remainder;

    //Approximate Cost of evaluating the expression
    double cost;

    MySQLQueryManager mySQLQueryManager = new MySQLQueryManager();

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

    public void selectGuards(){
        Set<ObjectCondition> singletonSet = this.expression.getPolicies().stream()
                .flatMap(p -> p.getObject_conditions().stream())
                .filter(o -> PolicyConstants.INDEXED_ATTRS.contains(o.getAttribute()))
                .collect(Collectors.toSet());
        selectFactor(singletonSet);
    }

    /**
     * Factorization based on a single object condition and not all the possible combinations
     * After selecting factor, they are not removed from the quotient
     */
    public void selectFactor(Set<ObjectCondition> objectConditionSet) {
        Boolean factorized = false;
        FactorSelection currentBestFactor = new FactorSelection(this.expression);
        currentBestFactor.setCost(Double.POSITIVE_INFINITY);
        Set<ObjectCondition> removal = new HashSet<>();
        for (ObjectCondition objectCondition : objectConditionSet) {
            BEExpression temp = new BEExpression(this.expression);
            temp.checkAgainstPolices(objectCondition);
            if (temp.getPolicies().size() > 1) { //was able to factorize
                double tCost = temp.estimateCost();
//                System.out.println(String.format("Expression: %s, Cost: %s", temp.createQueryFromPolices(), tCost));
                double fCost = estimateCostOfGuardRep(objectCondition, temp);
//                System.out.println(String.format("Guard: %s, Partition: %s, Cost: %s", objectCondition.print(),
//                        temp.createQueryFromPolices(), fCost));
                if (tCost > fCost) {
                    if(currentBestFactor.cost > fCost) {
                        factorized = true;
                        currentBestFactor.multiplier = new ArrayList<>();
                        currentBestFactor.multiplier.add(objectCondition);
                        currentBestFactor.remainder = new FactorSelection(this.expression);
                        currentBestFactor.remainder.expression.getPolicies().removeAll(temp.getPolicies());
                        currentBestFactor.quotient = new FactorSelection(temp);
//                        currentBestFactor.quotient.expression.removeFromPolicies(objectCondition);
                        currentBestFactor.cost = fCost;
                    }
                }
                else removal.add(objectCondition); //not considered for factorization recursively
            }
        }
        if(factorized){
            this.setMultiplier(currentBestFactor.getMultiplier());
            this.setQuotient(currentBestFactor.getQuotient());
            this.setRemainder(currentBestFactor.getRemainder());
            this.setCost(currentBestFactor.cost);
            removal.add(this.getMultiplier().get(0));
            objectConditionSet.removeAll(removal);
            this.remainder.selectFactor(objectConditionSet);
        }
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
     *
     * @return
     */
    public double estimateCostOfGuardRep(ObjectCondition guard, BEExpression partition) {
        long numOfPreds = partition.getPolicies().stream().map(BEPolicy::getObject_conditions).mapToInt(List::size).sum();
        return PolicyConstants.NUMBER_OR_TUPLES * guard.computeL() * (PolicyConstants.IO_BLOCK_READ_COST +
                 PolicyConstants.ROW_EVALUATE_COST * 2 * numOfPreds * PolicyConstants.NUMBER_OF_PREDICATES_EVALUATED);
    }


    /**
     * returns a map with key as guards and value as the guarded representation of the partition of policies
     * guard is a single object condition
     *
     * @return
     */
    public HashMap<ObjectCondition, BEExpression> getGuardPartitionMap() {
        if (this.getMultiplier().isEmpty()) {
            HashMap<ObjectCondition, BEExpression> remainderMap = new HashMap<>();
            for (BEPolicy bePolicy : this.expression.getPolicies()) {
                double freq = PolicyConstants.NUMBER_OR_TUPLES;
                ObjectCondition gOC = new ObjectCondition();
                for (ObjectCondition oc : bePolicy.getObject_conditions()) {
                    if (!PolicyConstants.INDEXED_ATTRS.contains(oc.getAttribute())) continue;
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
        if (!this.getRemainder().expression.getPolicies().isEmpty()) {
            gMap.putAll(this.getRemainder().getGuardPartitionMap());
        }
        return gMap;
    }

    /**
     * Creates a query by AND'ing the guard and partition
     *
     * @param guard
     * @param partition
     * @return
     */
    public String createQueryFromGQ(ObjectCondition guard, BEExpression partition) {
        StringBuilder query = new StringBuilder();
        query.append(guard.print());
        query.append(PolicyConstants.CONJUNCTION);
        query.append("(");
        query.append(partition.createQueryFromPolices());
        query.append(")");
//        System.out.println(query.toString());
        return query.toString();
    }


    /**
     * Computes the cost of execution of individual guards and sums them up
     * For the remainder it considers the predicate with highest selectivity as the guard and computes the cost
     *
     * @return
     */
    public Duration computeGuardCosts() {
        Map<ObjectCondition, BEExpression> gMap = getGuardPartitionMap();
        Duration rcost = Duration.ofNanos(0);
        for (ObjectCondition kOb : gMap.keySet()) {
            Duration gCost = mySQLQueryManager.runTimedQuery(createQueryFromGQ(kOb, gMap.get(kOb)));
            rcost = rcost.plus(gCost);
        }
        return rcost;
    }

    public List<ObjectCondition> getIndexFilters(){
        if (multiplier.isEmpty()) {
            return multiplier;
        }
        List<ObjectCondition> indexFilters = new ArrayList<>();
        for (ObjectCondition mul : multiplier) {
            indexFilters.add(mul);
        }
        if (!this.remainder.expression.getPolicies().isEmpty()) {
            indexFilters.addAll(this.remainder.getIndexFilters());
        }
        return indexFilters;
    }
}
