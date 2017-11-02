package model.guard;

import common.PolicyConstants;
import common.PolicyEngineException;
import db.MySQLQueryManager;
import model.policy.BEExpression;
import model.policy.BEPolicy;
import model.policy.ObjectCondition;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by cygnus on 10/29/17.
 *
 * E = D.Q + R
 * where D is an exact factor
 */
public class ExactFactor{

    MySQLQueryManager queryManager = new MySQLQueryManager();

    //Original expression
    BEExpression expression;

    // Chosen factor
    ObjectCondition factor;

    // Polices from the original expression that contain the factor
    BEExpression quotient;

    // Polices from the original expression that does not contain the factor
    BEExpression reminder;

    //Cost of evaluating the expression
    Long cost;

    public ExactFactor(){
        this.expression = new BEExpression();
        this.factor = new ObjectCondition();
        this.quotient = new BEExpression();
        this.reminder = new BEExpression();
        this. cost = PolicyConstants.INFINTIY;
    }

    public ExactFactor(BEExpression expression){
        this.expression = new BEExpression(expression);
        this.factor = new ObjectCondition();
        this.quotient = new BEExpression();
        this.reminder = new BEExpression();
        this.cost = PolicyConstants.INFINTIY;
    }


    public BEExpression getExpression() {
        return expression;
    }

    public void setExpression(BEExpression expression) {
        this.expression = expression;
    }

    public ObjectCondition getFactor() {
        return factor;
    }

    public void setFactor(ObjectCondition factor) {
        this.factor = factor;
    }

    public BEExpression getQuotient() {
        return quotient;
    }

    public void setQuotient(BEExpression quotient) {
        this.quotient = quotient;
    }

    public BEExpression getReminder() {
        return reminder;
    }

    public void setReminder(BEExpression reminder) {
        this.reminder = reminder;
    }

    public Long getCost() {
        return cost;
    }

    public void setCost(Long cost) {
        this.cost = cost;
    }

    public void factorize(ObjectCondition oc) {
        BEExpression candidate = new BEExpression(this.getExpression());
        candidate.checkAgainstPolicies(oc);
        if (candidate.getPolicies().size() > 1) { //was able to factor
            this.factor = oc;
            this.quotient = new BEExpression(candidate);
            this.quotient.removeFromPolicies(oc);
            this.reminder = new BEExpression(this.getExpression());
            this.reminder.removePolicies(candidate.getPolicies());
            this.cost = queryManager.runTimedQuery(this.createQueryFromExactFactor());

        }
        else
            throw new PolicyEngineException("Couldn't factor the expression using repeating object condition");
    }
    
    /**
     * Creates a query string from Exact Factor by
     * AND'ing factor, quotient and by OR'ing the reminder
     * @return
     */
    public String createQueryFromExactFactor(){
        StringBuilder query = new StringBuilder();
        query.append(this.factor.print());
        query.append(PolicyConstants.CONJUNCTION);
        query.append(this.quotient.createQueryFromPolices());
        query.append(PolicyConstants.DISJUNCTION);
        query.append(this.reminder.createQueryFromPolices());
        return query.toString();
    }

}