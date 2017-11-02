package model.policy;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by cygnus on 10/26/17.
 */
public class BooleanPredicate implements Comparable<BooleanPredicate> {

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

    public BooleanPredicate(String value, String operator) {
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
    public int compareTo(BooleanPredicate bp) {
        if(this.getValue().equals(bp.getValue())) {
            if (this.getOperator().equals(bp.getOperator()))
                return 0;
        }
        return -1;
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
}
