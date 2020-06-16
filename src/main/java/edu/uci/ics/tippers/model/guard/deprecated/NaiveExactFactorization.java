package edu.uci.ics.tippers.model.guard.deprecated;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.dbms.QueryManager;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by cygnus on 10/29/17.
 *
 * E = D.Q + R
 * where D is an exact multiple
 */
public class NaiveExactFactorization {

    //Original expression
    BEExpression expression;

    // Chosen multiple
    List<ObjectCondition> multiplier;

    // Polices from the original expression that contain the multiple
    NaiveExactFactorization quotient;

    // Polices from the original expression that does not contain the multiple
    NaiveExactFactorization reminder;

    //Cost of evaluating the expression
    Long cost;

    QueryManager queryManager = new QueryManager();

    public NaiveExactFactorization(){
        this.expression = new BEExpression();
        this.multiplier = new ArrayList<ObjectCondition>();
        this. cost = PolicyConstants.INFINTIY;
    }

    public NaiveExactFactorization(BEExpression expression){
        this.expression = new BEExpression(expression);
        this.multiplier = new ArrayList<ObjectCondition>();
        this.cost = PolicyConstants.INFINTIY;
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

    public void setMultiple(List<ObjectCondition> multiplier) {
        this.multiplier = multiplier;
    }

    public NaiveExactFactorization getQuotient() {
        return quotient;
    }

    public void setQuotient(NaiveExactFactorization quotient) {
        this.quotient = quotient;
    }

    public NaiveExactFactorization getReminder() {
        return reminder;
    }

    public void setReminder(NaiveExactFactorization reminder) {
        this.reminder = reminder;
    }

    public Long getCost() {
        return cost;
    }

    public void setCost(Long cost) {
        this.cost = cost;
    }


    /**
     * NaiveExactFactorization using the collection of Object conditions
     * ------------------------------------------------------
     * 1) Checks if more than 1 policy in the expression contains the set of object conditions
     * 2) If yes, the set is chosen as multiplier and
     *    quotient is the list of policies after removing the set from the matched policies and
     *    reminder is rest of the expression after deleting policies
     * 3) Cost is assigned as the run time of the factorized expression
     * @param objSet
     */
    public void factorizeSet(Set<ObjectCondition> objSet){
        BEExpression qoutientWithMultiplier = new BEExpression(this.getExpression());
        qoutientWithMultiplier.checkAgainstPolices(objSet);
        if(qoutientWithMultiplier.getPolicies().size() > 1 ) { //was able to factorize
            this.multiplier = new ArrayList<>(objSet);
            this.quotient = new NaiveExactFactorization(qoutientWithMultiplier);
            this.quotient.getExpression().removeFromPolicies(objSet);
            this.reminder = new NaiveExactFactorization(this.getExpression());
            this.reminder.getExpression().getPolicies().removeAll(qoutientWithMultiplier.getPolicies());
            this.cost = queryManager.runTimedQuery(this.createQueryFromExactFactor() ).toMillis();
        }
    }

    /**
     * Recursive algorithm to (exact) factorino the expression
     * -------------------------------------------------------
     * 1) For each policy in the expression, generate a power set of all the object conditions
     * 2) For each element in the power set, factorize the expression
     * 3) After factorizing, if the multiplier is non-empty and
     *    cost of the factorized expression is lower than the previous factorization
     * 4) Assign it as the best factor
     * 5) Recursively factorize the quotient and reminder
     * 6) If no more policies left to factorize, break
     * TODO: Add a boolean flag to factorization after one step to return the best single factor
     */
    public void greedyFactorization(){
        if(this.expression.getPolicies().isEmpty()) return;
        for (int i = 0; i < expression.getPolicies().size(); i++) {
            BEPolicy bp = expression.getPolicies().get(i);
            NaiveExactFactorization currentFactor = new NaiveExactFactorization(this.expression);
            Set<Set<ObjectCondition>> powerSet = bp.calculatePowerSet();
            for (Set<ObjectCondition> objSet: powerSet) {
                if(objSet.size() == 0 || objSet.size() == bp.getObject_conditions().size()) continue;
                currentFactor.factorizeSet(objSet);
                if(currentFactor.getMultiplier().isEmpty()) continue;
                if (this.getCost() > currentFactor.getCost()){
                    this.multiplier = currentFactor.getMultiplier();
                    this.quotient = currentFactor.getQuotient();
                    this.quotient.greedyFactorization();;
                    this.reminder = currentFactor.getReminder();
                    this.reminder.greedyFactorization();
                    this.cost = queryManager.runTimedQuery(this.createQueryFromExactFactor()).toMillis();
                }
            }
        }
    }

    
    /**
     * Creates a query string from Exact Factor by
     * AND'ing object conditions in multiplier,
     * AND'ing the quotient with multiplier and by
     * OR'ing the reminder to the expression
     * Parenthesis are important to denote the evaluation order
     * @return
     */
    public String createQueryFromExactFactor(){
        if (multiplier.isEmpty()){
            if(expression != null) {
                return this.expression.createQueryFromPolices();
            }
            else
                return "";
        }
        StringBuilder query = new StringBuilder();
        for (ObjectCondition mul: multiplier) {
            query.append(mul.print());
            query.append(PolicyConstants.CONJUNCTION);
        }
        query.append("(");
        query.append(this.quotient.createQueryFromExactFactor());
        query.append(")");
        if(!this.reminder.expression.getPolicies().isEmpty())  {
            query.append(PolicyConstants.DISJUNCTION);
            query.append("(");
            query.append(this.reminder.createQueryFromExactFactor());
            query.append(")");
        }
        return query.toString();
    }

    public String printExactFactor(int n){
        if (multiplier.isEmpty()){
            if(expression != null) {
                return this.expression.createQueryFromPolices();
            }
            else
                return "";
        }
        StringBuilder query = new StringBuilder();
        for (ObjectCondition mul: multiplier) {
            query.append("multiplier " + n + ":");
            query.append(mul.print());
            query.append(PolicyConstants.CONJUNCTION);
            query.append("\n");
        }
        query.append("quotient "+ n + ":");
        query.append("(");
        query.append(this.quotient.printExactFactor(n+1));
        query.append(")");
        query.append("\n");
        if(!this.reminder.expression.getPolicies().isEmpty())  {
            query.append("reminder " + n + ":");
            query.append(PolicyConstants.DISJUNCTION);
            query.append("(");
            query.append(this.reminder.printExactFactor(n+1));
            query.append(")");
            query.append("\n");
        }
        return query.toString();
    }
}