package edu.uci.ics.tippers.model.policy;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.uci.ics.tippers.common.AttributeType;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import org.w3c.dom.Attr;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
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
            r.append("(" + this.getAttribute() + bp.getOperator() + " \'" + bp.getValue() + "\'" + ")");
            delim = PolicyConstants.CONJUNCTION;
        }
        return r.toString();
    }

    /**
     * Prints the predicate as a range
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

    @Override
    public String toString() {
        return  "policyID= " + policy_id +
                " attribute='" + attribute +
                ", booleanPredicates=" + booleanPredicates +
                '}';
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
        if(booleanCondition.getType() == AttributeType.INTEGER){
            int start1 = Integer.parseInt(this.getBooleanPredicates().get(0).getValue());
            int end1 = Integer.parseInt(this.getBooleanPredicates().get(1).getValue());
            int start2 = Integer.parseInt(booleanCondition.getBooleanPredicates().get(0).getValue());
            int end2 = Integer.parseInt(booleanCondition.getBooleanPredicates().get(1).getValue());
            return start1 != start2? start1 - start2: end1 - end2;
        }
        else if(booleanCondition.getType() == AttributeType.DATE) {
            LocalDate start1 = LocalDate.parse(this.getBooleanPredicates().get(0).getValue());
            LocalDate end1 = LocalDate.parse(this.getBooleanPredicates().get(1).getValue());
            LocalDate start2 = LocalDate.parse(booleanCondition.getBooleanPredicates().get(0).getValue());
            LocalDate end2 = LocalDate.parse(booleanCondition.getBooleanPredicates().get(1).getValue());
            if (!start1.equals(start2)) return start1.compareTo(start2);
            return end1.compareTo(end2);
        }
        else if (booleanCondition.getType() == AttributeType.TIME) {
            LocalTime start1 = LocalTime.parse(this.getBooleanPredicates().get(0).getValue());
            LocalTime end1 = LocalTime.parse(this.getBooleanPredicates().get(1).getValue());
            LocalTime start2 = LocalTime.parse(booleanCondition.getBooleanPredicates().get(0).getValue());
            LocalTime end2 = LocalTime.parse(booleanCondition.getBooleanPredicates().get(1).getValue());
            if (!start1.equals(start2)) return start1.compareTo(start2);
            return end1.compareTo(end2);
        }
        else if(booleanCondition.getType() == AttributeType.STRING) {
            String start1 = this.getBooleanPredicates().get(0).getValue();
            String end1 = this.getBooleanPredicates().get(1).getValue();
            String start2 = booleanCondition.getBooleanPredicates().get(0).getValue();
            String end2 = booleanCondition.getBooleanPredicates().get(1).getValue();
            return start1.compareTo(start2) != 0? start1.compareTo(start2) : end1.compareTo(end2);
        }
        else if(booleanCondition.getType() == AttributeType.DOUBLE) {
            double start1 = Double.parseDouble(this.getBooleanPredicates().get(0).getValue());
            double end1 = Double.parseDouble(this.getBooleanPredicates().get(1).getValue());
            double start2 = Double.parseDouble(booleanCondition.getBooleanPredicates().get(0).getValue());
            double end2 = Double.parseDouble(booleanCondition.getBooleanPredicates().get(1).getValue());
            return (int) (start1 != start2? start1 - start2 : end1 - end2);
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
        BooleanCondition bc = (BooleanCondition) obj;
        return bc.policy_id.equals(policy_id) && bc.attribute.equals(attribute) && bc.type.equals(type)
                && new HashSet<>(bc.booleanPredicates).equals(new HashSet<>(booleanPredicates));
    }


    public boolean equalsWithoutId(BooleanCondition bc){
        return bc.attribute.equals(attribute) && bc.type.equals(type) && new HashSet<>(bc.booleanPredicates).equals(new HashSet<>(booleanPredicates));
    }
}
