package edu.uci.ics.tippers.model.policy;

import edu.uci.ics.tippers.common.AttributeType;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import edu.uci.ics.tippers.model.guard.Bucket;
import edu.uci.ics.tippers.model.guard.Histogram;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by cygnus on 9/25/17.
 */
public class ObjectCondition extends BooleanCondition {

    public ObjectCondition() {

    }

    public ObjectCondition(ObjectCondition oc){
        this.attribute = oc.getAttribute();
        this.type = oc.getType();
        this.booleanPredicates = new ArrayList<BooleanPredicate>(oc.getBooleanPredicates().size());
        for(BooleanPredicate bp: oc.getBooleanPredicates()){
            this.booleanPredicates.add(new BooleanPredicate(bp));
        }
    }

    public ObjectCondition(String attribute, AttributeType attributeType, String o1, String v1, String o2, String v2){
        this.attribute = attribute;
        this.type = attributeType;
        List<BooleanPredicate> booleanPredicates = new ArrayList<>();
        booleanPredicates.add(new BooleanPredicate(o1, v1));
        booleanPredicates.add(new BooleanPredicate(o2, v2));
        this.booleanPredicates = booleanPredicates;
    }

    public double computeL(){
        List mBuckets = null;
        double selectivity = 0.0001;
        if(this.getAttribute().equalsIgnoreCase(PolicyConstants.TEMPERATURE_ATTR) ||
                this.getAttribute().equalsIgnoreCase(PolicyConstants.ENERGY_ATTR)){
            for (int i = 0; i < Histogram.getInstance().getBucketMap().get(this.getAttribute()).size(); i++) {
                Bucket b = Histogram.getInstance().getBucketMap().get(this.getAttribute()).get(i);
                if (Integer.parseInt(b.getValue()) >=
                        Integer.parseInt(this.getBooleanPredicates().get(0).getValue())){
                    selectivity += b.getFreq();
                }
            }
        }
        else if (this.getAttribute().equalsIgnoreCase(PolicyConstants.LOCATIONID_ATTR) ||
                this.getAttribute().equalsIgnoreCase(PolicyConstants.ACTIVITY_ATTR)){
            Bucket bucket = Histogram.getInstance().getBucketMap().get(this.getAttribute()).stream()
                    .filter(b -> b.getValue().equalsIgnoreCase(this.getBooleanPredicates().get(0).getValue()))
                    .findFirst()
                    .orElse(null);
            if (bucket != null) selectivity += bucket.getFreq();

        }
        else if (this.getAttribute().equalsIgnoreCase(PolicyConstants.USERID_ATTR)){
            Bucket bucket = Histogram.getInstance().getBucketMap().get(this.getAttribute()).stream()
                    .filter(b -> Integer.parseInt(b.getLower()) >=
                            Integer.parseInt(this.getBooleanPredicates().get(0).getValue())
                            && Integer.parseInt(b.getUpper()) <=
                            Integer.parseInt(this.getBooleanPredicates().get(1).getValue()))
                    .findFirst()
                    .orElse(null);
            if(bucket != null) selectivity += bucket.getFreq()/bucket.getNumberOfItems();

        }
        else if (this.getAttribute().equalsIgnoreCase(PolicyConstants.TIMESTAMP_ATTR)){
            //TODO: Overestimates the selectivity as the partially contained buckets are completely counted
            for (int i = 0; i < Histogram.getInstance().getBucketMap().get(this.getAttribute()).size(); i++) {
                Bucket b = Histogram.getInstance().getBucketMap().get(this.getAttribute()).get(i);
                if (timestampStrToCal(b.getLower()).compareTo(timestampStrToCal(this.getBooleanPredicates().get(1).getValue())) < 0
                        && timestampStrToCal(b.getUpper()).compareTo(timestampStrToCal(this.getBooleanPredicates().get(0).getValue())) > 0){
                    selectivity += b.getFreq();
                }
            }
        }
        else {
            throw new PolicyEngineException("Unknown attribute");
        }
        return selectivity/100; //As the frequency is in percentage, to convert it to ratio
    }

}
