package edu.uci.ics.tippers.model.policy;

import edu.uci.ics.tippers.common.AttributeType;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import edu.uci.ics.tippers.model.guard.Bucket;
import edu.uci.ics.tippers.db.Histogram;

import java.util.ArrayList;
import java.util.Collections;
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

    private double singletonRange(){
        double frequency = 0.0001;
        for (int i = 0; i < Histogram.getInstance().getBucketMap().get(this.getAttribute()).size(); i++) {
            Bucket b = Histogram.getInstance().getBucketMap().get(this.getAttribute()).get(i);
            if (Integer.parseInt(b.getValue()) >=
                    Integer.parseInt(this.getBooleanPredicates().get(0).getValue())){
                frequency += b.getFreq();
            }
        }
        return frequency/100;
    }

    private double singletonEquality(){
        double frequency = 0.0001;
        Bucket bucket = Histogram.getInstance().getBucketMap().get(this.getAttribute()).stream()
                .filter(b -> b.getValue().equalsIgnoreCase(this.getBooleanPredicates().get(0).getValue()))
                .findFirst()
                .orElse(null);
        if (bucket != null) frequency += bucket.getFreq();
        return frequency/100;
    }

    private double equiheightEquality(){
        double frequency = 0.0001;
        Bucket bucket = Histogram.getInstance().getBucketMap().get(this.getAttribute()).stream()
                .filter(b -> Integer.parseInt(b.getLower()) >=
                        Integer.parseInt(this.getBooleanPredicates().get(0).getValue())
                        && Integer.parseInt(b.getUpper()) <=
                        Integer.parseInt(this.getBooleanPredicates().get(1).getValue()))
                .findFirst()
                .orElse(null);
        if(bucket != null) frequency += bucket.getFreq()/bucket.getNumberOfItems();
        return frequency/100;
    }

    //TODO: Overestimates the selectivity as the partially contained buckets are completely counted
    private double equiheightRange(){
        double frequency = 0.0001;
        List<Bucket> buckets = Histogram.getInstance().getBucketMap().get(this.attribute);
        Bucket lKey = new Bucket();
        lKey.setAttribute(PolicyConstants.TIMESTAMP_ATTR);
        lKey.setLower(this.getBooleanPredicates().get(0).getValue());
        Bucket uKey = new Bucket();
        uKey.setAttribute(PolicyConstants.TIMESTAMP_ATTR);
        uKey.setLower(this.getBooleanPredicates().get(1).getValue());
        int lIndex = Collections.binarySearch(buckets, lKey);
        if (lIndex < 0) { // no exact match
            if(-lIndex > 2) {
                lIndex = -lIndex - 2;
            }
            else {//first bucket
                lIndex = -lIndex - 1;
            }
        }
        int uIndex = Collections.binarySearch(buckets, uKey);
        if (uIndex < 0) { // no exact match
            if(-uIndex > 2) {
                uIndex = -uIndex - 2;
            }
            else {//first bucket
                uIndex = -uIndex - 1;
            }
        }
        if(lIndex > buckets.size()-1 ) return frequency;
        if(uIndex > buckets.size()-1) uIndex = buckets.size() - 1;
        for (int i = lIndex; i <= uIndex; i++) {
            frequency += (buckets.get(i).getFreq()/buckets.get(i).getNumberOfItems());
        }
        System.out.println(this.toString() + " : " + frequency);
        return frequency/100;
    }


    public double computeL(){
        List mBuckets = null;
        if(this.getAttribute().equalsIgnoreCase(PolicyConstants.TEMPERATURE_ATTR) ||
                this.getAttribute().equalsIgnoreCase(PolicyConstants.ENERGY_ATTR)){
            return singletonRange();
        }
        else if (this.getAttribute().equalsIgnoreCase(PolicyConstants.LOCATIONID_ATTR) ||
                this.getAttribute().equalsIgnoreCase(PolicyConstants.ACTIVITY_ATTR)){
            return singletonEquality();
        }
        else if (this.getAttribute().equalsIgnoreCase(PolicyConstants.USERID_ATTR)){
           return equiheightEquality();
        }
        else if (this.getAttribute().equalsIgnoreCase(PolicyConstants.TIMESTAMP_ATTR)){
           return equiheightRange();
        }
        else {
            throw new PolicyEngineException("Unknown attribute");
        }
    }

    @Override
    public String toString() {
        return "ObjectCondition{" +
                "attribute='" + attribute + '\'' +
                ", type=" + type +
                ", booleanPredicates=" + booleanPredicates +
                '}';
    }
}
