package edu.uci.ics.tippers.model.guard;

import com.sun.corba.se.spi.ior.ObjectKey;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import edu.uci.ics.tippers.db.MySQLQueryManager;
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

    public void factorizeSet(Set<ObjectCondition> objSet){
        BEExpression qoutientWithMultiplier = new BEExpression(this.getExpression());
        qoutientWithMultiplier.checkAgainstPolices(objSet);
        if(qoutientWithMultiplier.getPolicies().size() > 1 ) { //was able to factorize
            this.multiplier = new ArrayList<ObjectCondition>(objSet);
            this.quotient = new ExactFactor(qoutientWithMultiplier);
            this.quotient.getExpression().removeSetFromPolicies(objSet);
            this.reminder = new ExactFactor(this.getExpression());
            this.reminder.getExpression().removePolicies(qoutientWithMultiplier.getPolicies());
            this.cost = queryManager.runTimedQuery(this.createQueryFromExactFactor());
        }
    }

    /**
     * Factorization using set of object conditions
     * TODO: Add a boolean flag to factorization after one step to return the best single factor
     */
    public void greedyFactorization(){
        if(this.expression.getPolicies().isEmpty()) return;
        for (int i = 0; i < expression.getPolicies().size(); i++) {
            BEPolicy bp = expression.getPolicies().get(i);
            ExactFactor currentFactor = new ExactFactor(this.expression);
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
                    this.cost = queryManager.runTimedQuery(this.createQueryFromExactFactor());
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


}