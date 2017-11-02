package model.policy;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.sun.org.apache.xpath.internal.operations.Bool;
import common.AttributeType;
import common.PolicyConstants;
import common.PolicyEngineException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * Created by cygnus on 9/25/17.
 *
 * BooleanCondition within the same policy
 *
 * {
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

    @JsonProperty("attribute")
    protected String attribute;

    @JsonProperty("type")
    protected AttributeType type;

    @JsonProperty("predicates")
    protected List<BooleanPredicate> booleanPredicates;

    public BooleanCondition(BooleanCondition oc) {
    }

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

    public BooleanCondition(String attribute, List<BooleanPredicate> booleanPredicates) {
        this.attribute = attribute;
        this.booleanPredicates = booleanPredicates;
    }


    public BooleanCondition() {
    }

    public String check_type(String value) {
        switch(type.getID()){
            case 1: //String
                return " \"" + value + "\" ";
            case 2: //Timestamp
                return value;
            case 3: //Double
                return value;
            default:
                throw new PolicyEngineException("Unknown Type error");
        }
    }

    public String print(){
        StringBuilder r = new StringBuilder();
        BooleanPredicate bp;
        String delim = "";
        for (int i = 0; i < this.getBooleanPredicates().size(); i++){
            bp = this.getBooleanPredicates().get(i);
            r.append(delim);
            r.append(" (" + this.getAttribute() + bp.getOperator() + check_type(bp.getValue()) + ") ");
            delim = PolicyConstants.CONJUNCTION;
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

    /**
     * Check if a boolean condition is contained within a list
     * @param bcs
     * @return
     */
    public boolean containedInList(List<ObjectCondition> bcs){
        for (int i = 0; i < bcs.size(); i++) {
            if (this.compareTo(bcs.get(i)) == 0)
                return true;
        }
        return false;
    }

    /**
     * @param bps
     * @return true if they are equal, false if not
     */

    public boolean compareBooleanPredicates(List<BooleanPredicate> bps){
        if (bps.size() != this.getBooleanPredicates().size())
            return false;
        int count = 0;
        for (int i = 0; i < bps.size(); i++) {
            for (int j = 0; j < this.getBooleanPredicates().size() ; j++) {
                int flag = bps.get(i).compareTo(this.getBooleanPredicates().get(j));
                if (bps.get(i).compareTo(this.getBooleanPredicates().get(j)) == 0){
                    count++;
                }
            }
        }
        if (count == bps.size()) return true;
        return false;
    }

    @Override
    public int compareTo(BooleanCondition booleanCondition) {
        if(this.getAttribute().equals(booleanCondition.getAttribute())) {
            if (this.getType().equals(booleanCondition.getType())) {
                if (this.compareBooleanPredicates(booleanCondition.getBooleanPredicates())) {
                    return 0;
                }
            }
        }
        return -1;
    }

    @Override
    public int hashCode() {
        return attribute.hashCode() ^ type.hashCode() ^ this.booleanPredicates.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BooleanCondition))
            return false;

        BooleanCondition bc = (BooleanCondition) obj;
        return bc.attribute.equals(attribute) && bc.type.equals(type) && bc.booleanPredicates.equals(booleanPredicates);
    }
}
