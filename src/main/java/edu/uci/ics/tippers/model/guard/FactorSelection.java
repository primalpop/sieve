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

    // Chosen multiple
    List<ObjectCondition> multiplier;

    // Polices from the original expression that contain the multiple
    FactorSelection quotient;

    // Polices from the original expression that does not contain the multiple
    FactorSelection reminder;

    //Cost of evaluating the expression
    double cost;

    MySQLQueryManager mySQLQueryManager = new MySQLQueryManager();

    public FactorSelection() {
        this.expression = new BEExpression();
        this.multiplier = new ArrayList<ObjectCondition>();
        this.cost = -1L;
    }

    public FactorSelection(BEExpression beExpression) {
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

    public FactorSelection getQuotient() {
        return quotient;
    }

    public void setQuotient(FactorSelection quotient) {
        this.quotient = quotient;
    }

    public FactorSelection getReminder() {
        return reminder;
    }

    public void setReminder(FactorSelection reminder) {
        this.reminder = reminder;
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
        long gain = (long) ((quotient.getPolicies().size() - 1)* objectCondition.computeL() * PolicyConstants.NUMBER_OR_TUPLES);
        gain += original.computeL() * PolicyConstants.NUMBER_OR_TUPLES;
        gain -= quotient.computeL() * PolicyConstants.NUMBER_OR_TUPLES;
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
        double guardFreq = objectCondition.computeL() * PolicyConstants.NUMBER_OR_TUPLES;
        double filterFreq = quotient.computeL() - original.computeL() * PolicyConstants.NUMBER_OR_TUPLES;
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
        return quotient.getPolicies().size() * (1 /objectCondition.computeL());
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
        FactorSelection currentFactor = new FactorSelection(this.expression);
        for (Set<ObjectCondition> objSet : powerSet) {
            if (objSet.size() == 0) continue;
            BEExpression temp = new BEExpression(this.expression);
            temp.checkAgainstPolices(objSet);
            if (temp.getPolicies().size() > 1) { //was able to factorize
                currentFactor.multiplier = new ArrayList<>(objSet);
                currentFactor.quotient = new FactorSelection(temp);
                currentFactor.quotient.expression.removeFromPolicies(objSet);
                currentFactor.reminder = new FactorSelection(this.expression);
                currentFactor.reminder.expression.getPolicies().removeAll(temp.getPolicies());
//                currentFactor.cost = computeGain(temp, objSet, currentFactor.quotient.expression);
                if (this.cost < currentFactor.cost) {
                    this.multiplier = currentFactor.getMultiplier();
                    this.quotient = currentFactor.getQuotient();
                    this.reminder = currentFactor.getReminder();
                    factorized = true;
                }
                currentFactor = new FactorSelection(this.expression);
            }
        }
        if(!factorized || (this.reminder.getExpression().getPolicies().size() <= 1 ) || rounds >=2) return;
        this.reminder.GFactorizeOptimal(rounds + 1);
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
        FactorSelection currentFactor = new FactorSelection(this.expression);
        for (ObjectCondition objectCondition : singletonSet) {
//            if(!PolicyConstants.INDEXED_ATTRS.contains(objectCondition.getAttribute())) continue;
            BEExpression temp = new BEExpression(this.expression);
            temp.checkAgainstPolices(objectCondition);
            if (temp.getPolicies().size() > 1) { //was able to factorize
                currentFactor.multiplier = new ArrayList<>();
                currentFactor.multiplier.add(objectCondition);
                currentFactor.quotient = new FactorSelection(temp);
                currentFactor.quotient.expression.removeFromPolicies(objectCondition);
                currentFactor.reminder = new FactorSelection(this.expression);
                currentFactor.reminder.expression.getPolicies().removeAll(temp.getPolicies());
                currentFactor.cost = computeCriteria(objectCondition, currentFactor.quotient.getExpression());
                if (this.cost < currentFactor.cost) {
                    this.multiplier = currentFactor.getMultiplier();
                    this.quotient = currentFactor.getQuotient();
                    this.reminder = currentFactor.getReminder();
                    this.cost = currentFactor.cost;
                    factorized = true;
                }
                currentFactor = new FactorSelection(this.expression);
            }
        }
        if(!factorized) return;
        if((this.reminder.getExpression().getPolicies().size() <= 1 )) return;
        this.reminder.GFactorize();
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
        if (!this.reminder.expression.getPolicies().isEmpty()) {
            query.append(PolicyConstants.DISJUNCTION);
            query.append("(");
            query.append(this.reminder.createQueryFromExactFactor());
            query.append(")");
        }
        return query.toString();
    }

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
        if (!this.reminder.expression.getPolicies().isEmpty()) {
            indexFilters.addAll(this.reminder.getIndexFilters());
        }
        return indexFilters;
    }


    public String createQueryFromGQ(ObjectCondition objectCondition, BEExpression quotient){
        StringBuilder query = new StringBuilder();
        query.append(objectCondition.print());
        query.append(PolicyConstants.CONJUNCTION);
        query.append("(");
        query.append(quotient.createQueryFromPolices());
        query.append(")");
        return query.toString();
    }

    public Duration computeGuardCosts(){
        if(multiplier.isEmpty()){
            Duration rcost = Duration.ofMillis(0);
            for(BEPolicy bePolicy: this.expression.getPolicies()){
                rcost.plus(mySQLQueryManager.runTimedQuery(bePolicy.createQueryFromObjectConditions(), null));
            }
            return rcost;
        }
        return mySQLQueryManager.runTimedQuery(createQueryFromGQ(multiplier.get(0),
                this.quotient.getExpression()), null).plus(this.reminder.computeGuardCosts());
    }
}
