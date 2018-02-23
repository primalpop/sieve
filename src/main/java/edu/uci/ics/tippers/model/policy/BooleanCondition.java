package edu.uci.ics.tippers.model.policy;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.uci.ics.tippers.common.AttributeType;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
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

public class BooleanCondition  implements Comparable<BooleanCondition>  {

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


    public String check_type(String value) {
        switch(type.getID()){
            case 1: //String
                return " \"" + value + "\" ";
            case 2: //Timestamp
                return " \"" + value + "\" ";
            case 3: //Double
                return value;
            case 4:
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
            r.append("(" + this.getAttribute() + bp.getOperator() + check_type(bp.getValue()) + ")");
            delim = PolicyConstants.CONJUNCTION;
        }
        return r.toString();
    }

    public static Calendar timestampStrToCal(String timestamp) {
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat(PolicyConstants.TIMESTAMP_FORMAT);
        try {
            cal.setTime(sdf.parse(timestamp));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return cal;
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
            Calendar start1 = timestampStrToCal(this.getBooleanPredicates().get(0).getValue());
            Calendar end1 = timestampStrToCal(this.getBooleanPredicates().get(1).getValue());
            Calendar start2 = timestampStrToCal(booleanCondition.getBooleanPredicates().get(0).getValue());
            Calendar end2 = timestampStrToCal(booleanCondition.getBooleanPredicates().get(1).getValue());
            return start1.compareTo(start2);
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
