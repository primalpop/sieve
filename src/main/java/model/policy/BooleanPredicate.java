package model.policy;

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

    public BooleanPredicate(String value, String operator) {
        this.value = value;
        this.operator = operator;
    }

    public BooleanPredicate(){

    }
}
