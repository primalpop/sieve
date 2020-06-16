package edu.uci.ics.tippers.model.guard.deprecated;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.dbms.QueryManager;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.util.*;

public class Factorino {

    // Chosen multiplier
    List<ObjectCondition> multiplier;

    // Policies after factorizing the expression with multiplier
    BEExpression quotient;

    //Cost of evaluating the expression
    long cost;
    private QueryManager queryManager = new QueryManager();


    public Factorino(List<ObjectCondition> multiplier, BEExpression quotient) {
        this.multiplier = multiplier;
        this.quotient = quotient;
    }

    public Factorino() {
        multiplier = new ArrayList<>();
        quotient = new BEExpression();
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

    public boolean sameMultiplier(Factorino factorino){
        Set<ObjectCondition> set = new HashSet<>(this.multiplier);
        return set.size() == factorino.getMultiplier().size() && set.containsAll(factorino.getMultiplier());
    }
}
