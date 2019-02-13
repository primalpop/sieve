package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.util.Objects;

public class Term implements Comparable<Term> {

    ObjectCondition factor;

    BEExpression quotient;

    BEExpression remainder;

    double fscore;

    double gscore;

    double hscore;

    public Term(){
        this.factor = new ObjectCondition();
        this.quotient = new BEExpression();
        this.remainder = new BEExpression();
        this.fscore = 0.0;
        this.gscore = 0.0;
        this.hscore = 0.0;
    }

    public Term(ObjectCondition factor, BEExpression quotient){
        this.factor = factor;
        this.quotient = quotient;
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

    public BEExpression getRemainder() {
        return remainder;
    }

    public void setRemainder(BEExpression remainder) {
        this.remainder = remainder;
    }

    public double getFscore() {
        return fscore;
    }

    public void setFscore(double fscore) {
        this.fscore = fscore;
    }

    public double getGscore() {
        return gscore;
    }

    public void setGscore(double gscore) {
        this.gscore = gscore;
    }

    public double getHscore() {
        return hscore;
    }

    public void setHscore(double hscore) {
        this.hscore = hscore;
    }

    @Override
    public int compareTo(Term o) {
        if(this.fscore == o.fscore)
            return 0;
        else return this.fscore > o.fscore ? 1: -1;
    }

    @Override
    public int hashCode() {
        return Objects.hash(factor, quotient, remainder);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        Term term = (Term) o;
        return this.factor.equals(term.factor) && this.quotient.equals(term.quotient)
                && this.remainder.equals(term.remainder);
    }

    /**
     * prints factor and quotient
     * @return query string
     */
    public String printFQ() {
        StringBuilder query = new StringBuilder();
        query.append(factor.print());
        query.append(PolicyConstants.CONJUNCTION);
        query.append("(");
        query.append(this.quotient.cleanQueryFromPolices());
        query.append(")");
        return query.toString();
    }

}
