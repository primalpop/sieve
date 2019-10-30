package edu.uci.ics.tippers.model.policy;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.stream.Stream;

/**
 * Created by cygnus on 10/26/17.
 */
public class BooleanPredicate {

    @JsonProperty("value")
    private String value;

    @JsonProperty("operator")
    private Operation operator;


    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Operation getOperator() {
        return operator;
    }

    public void setOperator(Operation operator) {
        this.operator = operator;
    }

    public BooleanPredicate(Operation operator, String value) {
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
        return bp.value.equalsIgnoreCase(value) && bp.operator.equals(operator);
    }

    @Override
    public String toString() {
        return value + operator;
    }

    public int compareOnType(BooleanPredicate o, String attribute) {
        if(Stream.of(PolicyConstants.USERID_ATTR).anyMatch(attribute::equalsIgnoreCase)) {
            int o1 = Integer.parseInt(this.getValue());
            int o2 = Integer.parseInt(o.getValue());
            return o1-o2;
        }
        else if(attribute.equalsIgnoreCase(PolicyConstants.START_TIME)) {
            LocalTime o1 = LocalTime.parse(this.getValue());
            LocalTime o2 = LocalTime.parse(o.getValue());
            return o1.compareTo(o2);
        }
        else if (attribute.equalsIgnoreCase(PolicyConstants.START_DATE)) {
            LocalDate o1 = LocalDate.parse(this.getValue());
            LocalDate o2 = LocalDate.parse(o.getValue());
            return o1.compareTo(o2);
        }
        else if (Stream.of(PolicyConstants.LOCATIONID_ATTR, PolicyConstants.GROUP_ATTR, PolicyConstants.PROFILE_ATTR)
                .anyMatch(attribute::equalsIgnoreCase))  {
            String o1 = this.getValue();
            String o2 = o.getValue();
            return o1.compareTo(o2);
        }
        else{
            throw new PolicyEngineException("Incompatible Attribute Type");
        }
    }

}
