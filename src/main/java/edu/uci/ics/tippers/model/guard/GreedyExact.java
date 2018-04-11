package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GreedyExact {

    //Original expression
    BEExpression expression;

    // Chosen multiple
    List<ObjectCondition> multiplier;

    // Polices from the original expression that contain the multiple
    GreedyExact quotient;

    // Polices from the original expression that does not contain the multiple
    GreedyExact reminder;

    //Cost of evaluating the expression
    Long cost;

    public GreedyExact(){
        this.expression = new BEExpression();
        this.multiplier = new ArrayList<ObjectCondition>();
        this. cost = PolicyConstants.INFINTIY;
    }

    public GreedyExact(BEExpression beExpression){
        this.expression = new BEExpression(beExpression);
        this.multiplier = new ArrayList<ObjectCondition>();
        this. cost = PolicyConstants.INFINTIY;
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

    public GreedyExact getQuotient() {
        return quotient;
    }

    public void setQuotient(GreedyExact quotient) {
        this.quotient = quotient;
    }

    public GreedyExact getReminder() {
        return reminder;
    }

    public void setReminder(GreedyExact reminder) {
        this.reminder = reminder;
    }

    public Long getCost() {
        return cost;
    }

    public void setCost(Long cost) {
        this.cost = cost;
    }

    public void GFactorize() {
        if(this.expression.getPolicies().isEmpty()) return;
        Set<Set<ObjectCondition>> powerSet = new HashSet<Set<ObjectCondition>>();
        for (int i = 0; i < this.expression.getPolicies().size(); i++) {
            BEPolicy bp = this.expression.getPolicies().get(i);
            Set<Set<ObjectCondition>> subPowerSet = bp.calculatePowerSet();
            powerSet.addAll(subPowerSet);
        }
        for (Set<ObjectCondition> objSet : powerSet) {
            if(objSet.size() == 0) continue;
            BEExpression temp = new BEExpression(this.expression);
            GreedyExact currentFactor = new GreedyExact(this.expression);
            temp.checkAgainstPolices(objSet);
            if (temp.getPolicies().size() > 1) { //was able to factorize
                currentFactor.multiplier = new ArrayList<>(objSet);
                currentFactor.quotient = new GreedyExact(temp);
                currentFactor.quotient.expression.removeFromPolicies(objSet);
                currentFactor.reminder = new GreedyExact(this.expression);
                currentFactor.reminder.expression.getPolicies().removeAll(temp.getPolicies());
                currentFactor.cost = MySQLQueryManager.runTimedQuery(this.createQueryFromExactFactor());
                if(this.cost > currentFactor.cost){
                    this.multiplier = currentFactor.getMultiplier();
                    this.quotient = currentFactor.getQuotient();
                    this.reminder = currentFactor.getReminder();
//                    this.reminder.GFactorize();
                }
            }
        }
    }


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
