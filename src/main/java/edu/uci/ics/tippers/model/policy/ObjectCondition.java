package edu.uci.ics.tippers.model.policy;

import edu.uci.ics.tippers.common.AttributeType;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import edu.uci.ics.tippers.model.guard.Bucket;
import edu.uci.ics.tippers.db.Histogram;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
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

    public boolean overlaps(ObjectCondition o2) {
        if (this.getType().getID() == 4) { //Integer
            int start1 = Integer.parseInt(this.getBooleanPredicates().get(0).getValue());
            int end1 = Integer.parseInt(this.getBooleanPredicates().get(1).getValue());
            int start2 = Integer.parseInt(o2.getBooleanPredicates().get(0).getValue());
            int end2 = Integer.parseInt(o2.getBooleanPredicates().get(1).getValue());
            if (start1 <= end2 && end1 >= start2)
                return true;
        } else if (this.getType().getID() == 2) { //Timestamp
            Long extension = (long) (60 * 1000); //1 minute extension
            Calendar start1 = timestampStrToCal(this.getBooleanPredicates().get(0).getValue());
            Calendar start1Ext = Calendar.getInstance();
            start1Ext.setTimeInMillis(start1.getTimeInMillis() - extension);
            Calendar end1 = timestampStrToCal(this.getBooleanPredicates().get(1).getValue());
            Calendar end1Ext = Calendar.getInstance();
            end1Ext.setTimeInMillis(end1.getTimeInMillis() + extension);
            Calendar start2 = timestampStrToCal(o2.getBooleanPredicates().get(0).getValue());
            Calendar start2Ext = Calendar.getInstance();
            start2Ext.setTimeInMillis(start2.getTimeInMillis() - extension);
            Calendar end2 = timestampStrToCal(o2.getBooleanPredicates().get(1).getValue());
            Calendar end2Ext = Calendar.getInstance();
            end2Ext.setTimeInMillis(end2.getTimeInMillis() + extension);
            if (start1Ext.compareTo(end2Ext) < 0 && end1Ext.compareTo(start2Ext) > 0) {
                return true;
            }
        } else if (this.getType().getID() == 1) { //String
            if (this.getAttribute().equalsIgnoreCase(PolicyConstants.USERID_ATTR)) {
                int start1 = Integer.parseInt(this.getBooleanPredicates().get(0).getValue());
                int end1 = Integer.parseInt(this.getBooleanPredicates().get(1).getValue());
                int start2 = Integer.parseInt(o2.getBooleanPredicates().get(0).getValue());
                int end2 = Integer.parseInt(o2.getBooleanPredicates().get(1).getValue());
                if (start1 - 1000 <= end2 + 1000 && end1 + 1000 >= start2 - 1000)
                    return true;
            } else if (this.getAttribute().equalsIgnoreCase(PolicyConstants.LOCATIONID_ATTR)) {
                int start1 = Integer.parseInt(this.getBooleanPredicates().get(0).getValue().substring(0, 4));
                int end1 = Integer.parseInt(this.getBooleanPredicates().get(1).getValue().substring(0, 4));
                int start2 = Integer.parseInt(o2.getBooleanPredicates().get(0).getValue().substring(0, 4));
                int end2 = Integer.parseInt(o2.getBooleanPredicates().get(1).getValue().substring(0, 4));
                if (start1 - 100 <= end2 + 100 && end1 + 100 >= start2 - 100)
                    return true;
            } else {
                //attribute activity
                return false;
            }
        } else {
            throw new PolicyEngineException("Incompatible Attribute Type");
        }
        return false;
    }

    //TODO: Remove this
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
}
