package model.guard;

import common.PolicyConstants;
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
        expression = new BEExpression();
        factor = new ObjectCondition();
        quotient = new BEExpression();
        reminder = new BEExpression();
        cost = PolicyConstants.INFINTIY;
    }

    public ExactFactor(ExactFactor ef){
        expression = ef.expression;
        factor = ef.factor;
        quotient = ef.quotient;
        reminder = ef.reminder;
        cost = ef.cost;
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
        BEExpression q = new BEExpression();
        q.setPolicies(this.expression.checkAgainstPolicies(oc));
        if (q.getPolicies().size() > 1) { //was able to factor
            this.factor = oc;
            this.quotient.setPolicies(q.removeFromPolicies(oc));
            this.reminder = expression;
            this.reminder.getPolicies().removeAll(q.getPolicies());
            this.cost = queryManager.runTimedQuery(this.createQueryFromExactFactor());

        }
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