package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.util.Objects;

public class Term implements Comparable<Term> {

    private ObjectCondition factor;

    private BEExpression quotient;

    private BEExpression remainder;

    private double fscore;

    private double gscore;

    private double hscore;

    private double benefit;

    private double utility;

    public Term(){
        this.factor = new ObjectCondition();
        this.quotient = new BEExpression();
        this.remainder = new BEExpression();
        this.fscore = 0.0;
        this.gscore = 0.0;
        this.hscore = 0.0;
        this.benefit = 0.0;
        this.utility = 0.0;
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

    public double getBenefit() {
        return benefit;
    }

    public void setBenefit(double benefit) {
        this.benefit = benefit;
    }

    public double getUtility() {
        return utility;
    }

    public void setUtility(double utility) {
        this.utility = utility;
    }

    /**
     * Used for sorting of priority queue
     * @param o
     * @return
     */
    @Override
    public int compareTo(Term o) {
        if(this.utility == o.utility)
            return 0;
        else return Double.compare(o.utility, this.utility);
    }

    @Override
    public int hashCode() {
        return Objects.hash(factor, quotient);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        Term term = (Term) o;
        return this.factor.equals(term.factor) && this.quotient.equals(term.quotient);
    }

    @Override
    public String toString() {
        return "Term{" +
                "factor=" + factor +
                ", utility=" + utility +
                '}';
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
        this.quotient.removeDuplicates();
        query.append(this.quotient.createQueryFromPolices());
        query.append(")");
        return query.toString();
    }

}
