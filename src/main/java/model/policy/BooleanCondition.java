package model.policy;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;


/**
 * Created by cygnus on 9/25/17.
 */
public class BooleanCondition {

    @JsonProperty("attribute")
    private String attribute;

    @JsonProperty("predicates")
    private List<BooleanPredicate> booleanPredicates;

    public String getAttribute() {
        return attribute;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }


    public List<BooleanPredicate> getBooleanPredicates() {
        return booleanPredicates;
    }

    public void setBooleanPredicates(List<BooleanPredicate> booleanPredicates) {
        this.booleanPredicates = booleanPredicates;
    }

    public BooleanCondition(String attribute, List<BooleanPredicate> booleanPredicates) {
        this.attribute = attribute;
        this.booleanPredicates = booleanPredicates;
    }

    public BooleanCondition() {
    }

    public String print(){
        StringBuilder r = new StringBuilder();
        BooleanPredicate bp;
        for (int i = 0; i < this.getBooleanPredicates().size(); i++){
            bp = this.getBooleanPredicates().get(i);
            r.append("(" + this.getAttribute() + bp.getOperator() + bp.getValue() + ")");
        }
        return r.toString();
    }

    public double getStart(){
        return Double.parseDouble(this.booleanPredicates.stream()
                .filter(x -> x.getOperator().equals(RelOperator.EQUALS.getName()) || x.getOperator().equals(RelOperator.GEQ.getName()))
                .map(BooleanPredicate::getValue)
                .findAny()
                .orElse(String.valueOf(Double.NEGATIVE_INFINITY)));
    }


    public double getEnd(){
        return Double.parseDouble(this.booleanPredicates.stream()
                .filter((x) -> x.getOperator().equals(RelOperator.EQUALS.getName()) || x.getOperator().equals(RelOperator.LEQ.getName()))
                .map(BooleanPredicate::getValue)
                .findFirst()
                .orElse(String.valueOf(Double.POSITIVE_INFINITY)));
    }


    public boolean checkOverlap(BooleanCondition bc) {
        if (! (this.getEnd() < bc.getStart() || this.getStart() > bc.getEnd())){
            return true;
        }
        return false;
    }

    public boolean checkSame(BooleanCondition bc) {
        if (this.getStart() == bc.getStart() && this.getEnd() == bc.getEnd())
            return true;
        return false;
    }



}
