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

    public ExactFactor(BEExpression beExpression){
        expression = beExpression;
        factor = new ObjectCondition();
        quotient = new BEExpression();
        reminder = new BEExpression();
        cost = PolicyConstants.INFINTIY;
    }



    public void factorize(){
        List<ObjectCondition> objectConditions = this.expression.getRepeating();
        for (ObjectCondition oc: objectConditions) {
            BEExpression q = new BEExpression();
            q.setPolicies(this.expression.checkAgainstPolicies(oc));
            if(q.getPolicies().size() > 1){ //was able to factor
                this.factor = oc;
                this.quotient.setPolicies(q.removeFromPolicies(oc));
                this.reminder = expression;
                this.reminder.getPolicies().removeAll(q.getPolicies());
                this.cost = queryManager.runTimedQuery(this.createQueryFromExactFactor());
            }
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