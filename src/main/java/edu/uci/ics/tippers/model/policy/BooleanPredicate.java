package edu.uci.ics.tippers.model.policy;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by cygnus on 10/26/17.
 */
public class BooleanPredicate {

    @JsonProperty("value")
    private String value;

    @JsonProperty("operator")
    private String operator;


    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public BooleanPredicate(String operator, String value) {
        this.value = value;
        this.operator = operator;
    }

    public BooleanPredicate(){

    }

    public BooleanPredicate(BooleanPredicate bp){
        this.value = bp.getValue();
        this.operator = bp.getOperator();
    }

    @Override
    public int hashCode() {
        return value.hashCode() ^ operator.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BooleanPredicate))
            return false;

        BooleanPredicate bp = (BooleanPredicate) obj;
        return bp.value.equals(value) && bp.operator.equals(operator);
    }

    @Override
    public String toString() {
        return "BooleanPredicate{" +
                "value='" + value + '\'' +
                ", operator='" + operator + '\'' +
                '}';
    }


}
