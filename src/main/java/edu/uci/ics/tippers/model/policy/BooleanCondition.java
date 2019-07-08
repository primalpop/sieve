package edu.uci.ics.tippers.model.policy;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.uci.ics.tippers.common.AttributeType;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.*;


/**
 * Created by cygnus on 9/25/17.
 *
 * BooleanCondition within the same policy
 *
 * {
 *      "policy_id": 001,
 *      "attribute": "semantic_observation.timeStamp",
 *      "type":   "TIMESTAMP",
 *      "predicates": [{
 *          "operator": ">=",
 *          "value": "17"
 *      },
 *      {
 *          "operator": "<=",
 *          "value": "19"
 *      }
 *    ]
 * }
 */

public class BooleanCondition  implements Comparable<BooleanCondition>  {

    @JsonProperty("policy_id")
    protected String policy_id;

    @JsonProperty("attribute")
    protected String attribute;

    @JsonProperty("type")
    protected AttributeType type;

    @JsonProperty("predicates")
    protected List<BooleanPredicate> booleanPredicates;

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

    public AttributeType getType() {
        return type;
    }

    public void setType(AttributeType type) {
        this.type = type;
    }


    public BooleanCondition() {
        this.booleanPredicates = new ArrayList<>();
    }

    public BooleanCondition(BooleanCondition booleanCondition) {
        this.booleanPredicates = new ArrayList<>();
        for(BooleanPredicate bp: booleanCondition.getBooleanPredicates()){
            this.booleanPredicates.add(new BooleanPredicate(bp));
        }
    }


    //TODO: Is this required? If not in quotes, it raises this warning
    //TODO: Cannot use range access on index 'so_t' due to type or collation conversion on field 'temperature'
    public String check_type(String value) {
        switch(type.getID()){
            case 1: //String
                return " \"" + value + "\" ";
            case 2: //Timestamp
                return " \"" + value + "\" ";
            case 3: //Double
                return " \"" + value + "\" ";
            case 4:
                return " \"" + value + "\" ";
            default:
                throw new PolicyEngineException("Unknown Type error");
        }
    }

    /**
     * Removes duplicate predicates
     * @return
     */
    public String print(){
        StringBuilder r = new StringBuilder();
        String delim = "";
        BooleanCondition dupElim = new BooleanCondition(this);
        Set<BooleanPredicate> og = new HashSet<>(dupElim.getBooleanPredicates());
        dupElim.getBooleanPredicates().clear();
        dupElim.getBooleanPredicates().addAll(og);
        for (BooleanPredicate bp: dupElim.getBooleanPredicates()) {
            r.append(delim);
            r.append("(" + this.getAttribute() + bp.getOperator() + check_type(bp.getValue()) + ")");
            delim = PolicyConstants.CONJUNCTION;
        }
        return r.toString();
    }

    /**
     * Removes duplicate predicates
     * @return
     */
    public String printRange(){
        StringBuilder r = new StringBuilder();
        BooleanCondition dupElim = new BooleanCondition(this);
        Set<BooleanPredicate> og = new HashSet<>(dupElim.getBooleanPredicates());
        dupElim.getBooleanPredicates().clear();
        dupElim.getBooleanPredicates().addAll(og);
        String lr = "", hr = "";
        for (BooleanPredicate bp: dupElim.getBooleanPredicates()) {
            if(bp.getOperator().equals(">=")) lr =  bp.getValue();
            if(bp.getOperator().equals("<=")) hr = bp.getValue();
        }
        r.append("[").append(lr).append(", ").append(hr).append("]");
        return r.toString();
    }


    //TODO: Compare it using String instead?
    public static LocalDateTime timeStampToLDT(String timestamp) {
        return LocalDateTime.parse(timestamp, PolicyConstants.TIME_FORMATTER);
    }

    @Override
    public String toString() {
        return attribute + booleanPredicates;
    }

    public String getPolicy_id() {
        return policy_id;
    }

    public void setPolicy_id(String policy_id) {
        this.policy_id = policy_id;
    }


    /**
     * 0 if they are equal, negative if start predicate of first boolean condition
     * is less than start predicate of second boolean condition, positive if vice versa.
     * @param booleanCondition
     * @return
     */
    @Override
    public int compareTo(BooleanCondition booleanCondition) {
        if(booleanCondition.getType().getID() == 4){ //Integer
            int start1 = Integer.parseInt(this.getBooleanPredicates().get(0).getValue());
            int end1 = Integer.parseInt(this.getBooleanPredicates().get(1).getValue());
            int start2 = Integer.parseInt(booleanCondition.getBooleanPredicates().get(0).getValue());
            int end2 = Integer.parseInt(booleanCondition.getBooleanPredicates().get(1).getValue());
            return start1 != start2? start1 - start2: end1- end2;
        }
        else if(booleanCondition.getType().getID() == 2) { //Timestamp
            LocalDateTime start1 = timeStampToLDT(this.getBooleanPredicates().get(0).getValue());
            LocalDateTime end1 = timeStampToLDT(this.getBooleanPredicates().get(1).getValue());
            LocalDateTime start2 = timeStampToLDT(booleanCondition.getBooleanPredicates().get(0).getValue());
            LocalDateTime end2 = timeStampToLDT(booleanCondition.getBooleanPredicates().get(1).getValue());
            if (!start1.equals(start2)) return start1.compareTo(start2);
            return end1.compareTo(end2);
        }
        else if(booleanCondition.getType().getID() == 1) {
            String start1 = this.getBooleanPredicates().get(0).getValue();
            String end1 = this.getBooleanPredicates().get(1).getValue();
            String start2 = booleanCondition.getBooleanPredicates().get(0).getValue();
            String end2 = booleanCondition.getBooleanPredicates().get(1).getValue();
            return start1.compareTo(start2) != 0? start1.compareTo(start2) : end1.compareTo(end2);
        }
        else{
            throw new PolicyEngineException("Incompatible Attribute Type");
        }
    }

    /**
     * Includes the policy_id so that if two identical object conditions belong to different policies, they generate
     * different hash codes
     * @return
     */
    @Override
    public int hashCode() {
        return policy_id.hashCode() ^ attribute.hashCode() ^ type.hashCode() ^ this.booleanPredicates.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BooleanCondition))
            return false;

        BooleanCondition bc = (BooleanCondition) obj;
        return bc.policy_id == (policy_id) && bc.attribute.equals(attribute) && bc.type.equals(type) && bc.booleanPredicates.equals(booleanPredicates);
    }


    public boolean equalsWithoutId(BooleanCondition bc){
        return bc.attribute.equals(attribute) && bc.type.equals(type) && bc.booleanPredicates.equals(booleanPredicates);
    }
}
