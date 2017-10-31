package model.policy;

import com.fasterxml.jackson.annotation.JsonProperty;
import common.AttributeType;
import common.PolicyConstants;
import common.PolicyEngineException;

import java.util.ArrayList;
import java.util.List;


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

public class BooleanCondition {

    @JsonProperty("attribute")
    private String attribute;

    @JsonProperty("type")
    private AttributeType type;

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
            if(this.getAttribute().equals(bcs.get(i).getAttribute())){
                if(this.getType().equals(bcs.get(i).getType())){
                    if(this.compareBooleanPredicates(bcs.get(i).getBooleanPredicates())){
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * https://stackoverflow.com/a/16209854
     * @param bps
     * @return true if they are equal, false if not
     */

    public boolean compareBooleanPredicates(List<BooleanPredicate> bps){
        if(this.getBooleanPredicates().size() != bps.size())
            return false;
        List<BooleanPredicate> cp = new ArrayList<BooleanPredicate>(this.getBooleanPredicates());
        for (BooleanPredicate bp: bps) {
            if( !cp.remove(bp))
                return false;
        }
        return cp.isEmpty();
    }
}
