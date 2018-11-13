package edu.uci.ics.tippers.model.policy;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;

import java.time.LocalDateTime;

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

    public int compareOnType(BooleanPredicate o, int type) {
        if(type == 4){ //Integer
            int o1 = Integer.parseInt(this.getValue());
            int o2 = Integer.parseInt(o.getValue());
            return o1-o2;
        }
        else if(type == 2) { //Timestamp
            LocalDateTime o1 = timeStampToLDT(this.getValue());
            LocalDateTime o2 = timeStampToLDT(o.getValue());
            return o1.compareTo(o2);
        }
        else if(type == 1) {
            String o1 = this.getValue();
            String o2 = o.getValue();
            return o1.compareTo(o2);
        }
        else{
            throw new PolicyEngineException("Incompatible Attribute Type");
        }
    }


    public static LocalDateTime timeStampToLDT(String timestamp) {
        return LocalDateTime.parse(timestamp, PolicyConstants.TIME_FORMATTER);
    }

}
