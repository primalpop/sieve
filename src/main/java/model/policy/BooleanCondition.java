package model.policy;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;


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

    //TODO: This is wrong as it compares objects

    public Boolean sameAs(BooleanCondition b){
        if(!this.getAttribute().equals(b.getAttribute()))
            return false;
        if(!this.getBooleanPredicates().containsAll(b.getBooleanPredicates()))
            return false;
        return true;
    }

}
