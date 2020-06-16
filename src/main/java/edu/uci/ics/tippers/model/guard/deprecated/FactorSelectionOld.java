package edu.uci.ics.tippers.model.guard.deprecated;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.dbms.QueryManager;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class FactorSelectionOld {

    //Original expression
    BEExpression expression;

    // Chosen multiple
    List<ObjectCondition> multiplier;

    // Polices from the original expression that contain the multiple
    FactorSelectionOld quotient;

    // Polices from the original expression that does not contain the multiple
    FactorSelectionOld remainder;

    //Cost of evaluating the expression
    double cost;

    QueryManager queryManager = new QueryManager();

    public FactorSelectionOld() {
        this.expression = new BEExpression();
        this.multiplier = new ArrayList<ObjectCondition>();
        this.cost = -1L;
    }

    public FactorSelectionOld(BEExpression beExpression) {
        this.expression = new BEExpression(beExpression);
        this.multiplier = new ArrayList<ObjectCondition>();
        this.cost = -1L;
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

    public FactorSelectionOld getQuotient() {
        return quotient;
    }

    public void setQuotient(FactorSelectionOld quotient) {
        this.quotient = quotient;
    }

    public FactorSelectionOld getRemainder() {
        return remainder;
    }

    public void setRemainder(FactorSelectionOld remainder) {
        this.remainder = remainder;
    }

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }

    /**
     * Computing gain for Exact Factorization of single predicate
     * Based on the gain formula from Surajit's paper
     * @param original
     * @param objectCondition
     * @param quotient
     * @return
     */
    public long computeGain(BEExpression original, ObjectCondition objectCondition, BEExpression quotient) {
        long gain = (long) ((quotient.getPolicies().size() - 1)* objectCondition.computeL() * PolicyConstants.getNumberOfTuples());
        gain += original.computeL() * PolicyConstants.getNumberOfTuples();
        gain -= quotient.computeL() * PolicyConstants.getNumberOfTuples();
        return gain;
    }


    /**
     * Computing benefit by computing the weighted normalized sum of number of tuples satisfied by guard
     * and difference of tuples between original and quotient/filter.
     * @param objectCondition
     * @param original
     * @param quotient
     * @return
     */
    public double computeBenefit(ObjectCondition objectCondition, BEExpression original, BEExpression quotient){
        double guardFreq = objectCondition.computeL() * PolicyConstants.getNumberOfTuples();
        double filterFreq = quotient.computeL() - original.computeL() * PolicyConstants.getNumberOfTuples();
        return ((0.8 * guardFreq) + (0.2 * filterFreq))/ (guardFreq + filterFreq);
    }

    /**
     *  n * 1 / (frequency of the factor)
     *  n - number of policies factorized
     *  Intuition: Guards that filter away higher number of tuples are better
     * @param objectCondition
     * @param quotient
     * @return
     */
    public double computeCriteria(ObjectCondition objectCondition, BEExpression quotient){
        return quotient.getPolicies().size();
//        return quotient.getPolicies().size() * (1 /objectCondition.computeL());
    }

    /**
     * Factorization based on set of object conditions
     * @param rounds
     */
    public void GFactorizeOptimal(int rounds) {
        Boolean factorized = false;
        Set<Set<ObjectCondition>> powerSet = new HashSet<Set<ObjectCondition>>();
        for (int i = 0; i < this.expression.getPolicies().size(); i++) {
            BEPolicy bp = this.expression.getPolicies().get(i);
            Set<Set<ObjectCondition>> subPowerSet = bp.calculatePowerSet();
            powerSet.addAll(subPowerSet);
            subPowerSet = null; //forcing garbage collection
        }
        FactorSelectionOld currentFactor = new FactorSelectionOld(this.expression);
        for (Set<ObjectCondition> objSet : powerSet) {
            if (objSet.size() == 0) continue;
            BEExpression temp = new BEExpression(this.expression);
            temp.checkAgainstPolices(objSet);
            if (temp.getPolicies().size() > 1) { //was able to factorize
                currentFactor.multiplier = new ArrayList<>(objSet);
                currentFactor.quotient = new FactorSelectionOld(temp);
                currentFactor.quotient.expression.removeFromPolicies(objSet);
                currentFactor.remainder = new FactorSelectionOld(this.expression);
                currentFactor.remainder.expression.getPolicies().removeAll(temp.getPolicies());
//                currentFactor.cost = computeGain(temp, objSet, currentFactor.quotient.expression);
                if (this.cost < currentFactor.cost) {
                    this.multiplier = currentFactor.getMultiplier();
                    this.quotient = currentFactor.getQuotient();
                    this.remainder = currentFactor.getRemainder();
                    factorized = true;
                }
                currentFactor = new FactorSelectionOld(this.expression);
            }
        }
        if(!factorized || (this.remainder.getExpression().getPolicies().size() <= 1 ) || rounds >=2) return;
        this.remainder.GFactorizeOptimal(rounds + 1);
    }

    /**
     * Factorization based on a single object condition and not all the possible combinations
     * TODO: How does exact factorization take the aspect of indices available into consideration?
     */
    public void GFactorize() {
        Boolean factorized = false;
        Set<ObjectCondition> singletonSet = this.expression.getPolicies().stream()
                .flatMap(p -> p.getObject_conditions().stream())
                .collect(Collectors.toSet());
        FactorSelectionOld currentFactor = new FactorSelectionOld(this.expression);
        for (ObjectCondition objectCondition : singletonSet) {
            if(!PolicyConstants.ATTRIBUTES.contains(objectCondition.getAttribute())) continue;
            BEExpression temp = new BEExpression(this.expression);
            temp.checkAgainstPolices(objectCondition);
            if (temp.getPolicies().size() > 1) { //was able to factorize
                currentFactor.multiplier = new ArrayList<>();
                currentFactor.multiplier.add(objectCondition);
                currentFactor.quotient = new FactorSelectionOld(temp);
                currentFactor.quotient.expression.removeFromPolicies(objectCondition);
                currentFactor.remainder = new FactorSelectionOld(this.expression);
                currentFactor.remainder.expression.getPolicies().removeAll(temp.getPolicies());
                currentFactor.cost = computeCriteria(objectCondition, currentFactor.quotient.getExpression());
                if (this.cost < currentFactor.cost) {
                    this.multiplier = currentFactor.getMultiplier();
                    this.quotient = currentFactor.getQuotient();
                    this.remainder = currentFactor.getRemainder();
                    this.cost = currentFactor.cost;
                    factorized = true;
                }
                currentFactor = new FactorSelectionOld(this.expression);
            }
        }
        if(!factorized) return;
        if((this.remainder.getExpression().getPolicies().size() <= 1 )) return;
        this.remainder.GFactorize();
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
     * Get the guards from the factorized expression
     * @return
     */
    public List<ObjectCondition> getIndexFilters(){
        if (multiplier.isEmpty()) {
            return multiplier;
        }
        List<ObjectCondition> indexFilters = new ArrayList<>();
        for (ObjectCondition mul : multiplier) {
            indexFilters.add(mul);
        }
        //TODO: Currently quotient not factorized
//        indexFilters.addAll(this.quotient.getIndexFilters());
        if (!this.remainder.expression.getPolicies().isEmpty()) {
            indexFilters.addAll(this.remainder.getIndexFilters());
        }
        return indexFilters;
    }

    /**
     * Creates a query by AND'ing the multiplier and quotient
     * @param objectCondition
     * @param quotient
     * @return
     */
    public String createQueryFromGQ(ObjectCondition objectCondition, BEExpression quotient){
        StringBuilder query = new StringBuilder();
        query.append(objectCondition.print());
        query.append(PolicyConstants.CONJUNCTION);
        query.append("(");
        query.append(quotient.createQueryFromPolices());
        query.append(")");
        return query.toString();
    }

    /**
     * Computes the cost of execution of individual guards and sums them up
     * For the remainder it computes the cost of each policy separately
     * @return
     */
    public Duration computeGuardCosts(){
        if(multiplier.isEmpty()){
            Duration rcost = Duration.ofMillis(0);
            for(BEPolicy bePolicy: this.expression.getPolicies()){
                rcost.plus(queryManager.runTimedQuery(bePolicy.createQueryFromObjectConditions() ));
            }
            return rcost;
        }
        return queryManager.runTimedQuery(createQueryFromGQ(multiplier.get(0),
                this.quotient.getExpression()) ).plus(this.remainder.computeGuardCosts());
    }

    /**
     * Computes the length of the unfactorized remainder
     * @return
     */
    public long lengthOfRemainder(){
        if(multiplier.isEmpty()){
            return expression.getPolicies().size();
        }
        return this.remainder.lengthOfRemainder();
    }
}
