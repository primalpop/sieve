package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.util.List;

public class Factorino {

    // Chosen multiplier
    List<ObjectCondition> multiplier;

    // Policies after factorizing the expression with multiplier
    BEExpression quotient;

    //Cost of evaluating the expression
    long cost;


    public Factorino(List<ObjectCondition> multiplier, BEExpression quotient) {
        this.multiplier = multiplier;
        this.quotient = quotient;
    }

    public Factorino() {

    }


    public List<ObjectCondition> getMultiplier() {
        return multiplier;
    }

    public void setMultiplier(List<ObjectCondition> multiplier) {
        this.multiplier = multiplier;
    }

    public BEExpression getQuotient() {
        return quotient;
    }

    public void setQuotient(BEExpression quotient) {
        this.quotient = quotient;
    }

    public Long getCost() {
        return cost;
    }

    public void setCost() {
        if (multiplier == null || quotient == null)
            this.cost = PolicyConstants.INFINTIY;
        else
            this.cost = MySQLQueryManager.runTimedQuery(this.toString());
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        for (ObjectCondition mul: multiplier) {
            sb.append(mul.print());
            sb.append(PolicyConstants.CONJUNCTION);
        }
        sb.append("(");
        sb.append(quotient.createQueryFromPolices());
        sb.append(")");
        return sb.toString();
    }
}
