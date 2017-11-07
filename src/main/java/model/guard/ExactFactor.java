package model.guard;

import common.PolicyConstants;
import common.PolicyEngineException;
import db.MySQLQueryManager;
import model.policy.BEExpression;
import model.policy.ObjectCondition;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by cygnus on 10/29/17.
 *
 * E = D.Q + R
 * where D is an exact multiple
 */
public class ExactFactor{

    MySQLQueryManager queryManager = new MySQLQueryManager();

    //Original expression
    BEExpression expression;

    // Chosen multiple
    List<ObjectCondition> multiplier;

    // Polices from the original expression that contain the multiple
    ExactFactor quotient;

    // Polices from the original expression that does not contain the multiple
    ExactFactor reminder;

    //Cost of evaluating the expression
    Long cost;

    public ExactFactor(){
        this.expression = new BEExpression();
        this.multiplier = new ArrayList<ObjectCondition>();
        this. cost = PolicyConstants.INFINTIY;
    }

    public ExactFactor(BEExpression expression){
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

    public ExactFactor getQuotient() {
        return quotient;
    }

    public void setQuotient(ExactFactor quotient) {
        this.quotient = quotient;
    }

    public ExactFactor getReminder() {
        return reminder;
    }

    public void setReminder(ExactFactor reminder) {
        this.reminder = reminder;
    }

    public Long getCost() {
        return cost;
    }

    public void setCost(Long cost) {
        this.cost = cost;
    }

    /**
     * Uses the given object condition to factorize the expression and set the quotient and reminder
     * TODO: Use list of object conditions instead of one object condition
     */
    public void factorize(ObjectCondition oc) {

        BEExpression qoutientWithMultiplier = new BEExpression(this.getExpression());
        qoutientWithMultiplier.checkAgainstPolicies(oc);
        if (qoutientWithMultiplier.getPolicies().size() > 1) { //was able to factorize
            this.multiplier.add(oc);
            this.quotient = new ExactFactor(qoutientWithMultiplier);
            this.quotient.getExpression().removeFromPolicies(oc);
            this.reminder = new ExactFactor(this.getExpression());
            this.reminder.getExpression().removePolicies(qoutientWithMultiplier.getPolicies());
            this.cost = queryManager.runTimedQuery(this.createQueryFromExactFactor());

        } else
            throw new PolicyEngineException("Couldn't factorize the expression using repeating object condition");

    }

    /**
     * For the exact factor, finds the best possible factorization using greedy approach
     */
    public void greedyFactorization() {
        List<ObjectCondition> objectConditions = this.expression.getRepeating();
        if (objectConditions.isEmpty()) return; //No more factorization possible

        for (ObjectCondition oc : objectConditions) {
            ExactFactor currentFactor = new ExactFactor(this.expression);
            currentFactor.factorize(oc);
            if (this.getCost() > currentFactor.getCost()) {
                this.multiplier = currentFactor.getMultiplier();
                this.quotient = currentFactor.getQuotient();
                this.quotient.greedyFactorization();
                this.reminder = currentFactor.getReminder();
                this.reminder.greedyFactorization();
                this.cost = queryManager.runTimedQuery(this.createQueryFromExactFactor());
            }
        }
    }

    /**
     * Finds the best multiplier and factorization using it
     * Does not recursively factorize the quotient or remainder
     */
    public void findBestFactor(){
        List<ObjectCondition> objectConditions = this.expression.getRepeating();
        if (objectConditions.isEmpty()) return; //No more factorization possible

        for (ObjectCondition oc : objectConditions) {
            ExactFactor currentFactor = new ExactFactor(this.expression);
            currentFactor.factorize(oc);
            if (this.getCost() > currentFactor.getCost()) {
                this.multiplier = currentFactor.getMultiplier();
                this.quotient = currentFactor.getQuotient();
                this.reminder = currentFactor.getReminder();
                this.cost = currentFactor.getCost();
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


}