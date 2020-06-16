package edu.uci.ics.tippers.model.policy;

import edu.uci.ics.tippers.common.AttributeType;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import edu.uci.ics.tippers.model.guard.Bucket;
import edu.uci.ics.tippers.dbms.mysql.Histogram;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Stream;

/**
 * Created by cygnus on 9/25/17.
 */
public class ObjectCondition extends BooleanCondition {

    public ObjectCondition(){

    }

    public ObjectCondition(ObjectCondition objectCondition){
        this.policy_id = objectCondition.getPolicy_id();
        this.attribute = objectCondition.getAttribute();
        this.type = objectCondition.getType();
        this.booleanPredicates = objectCondition.getBooleanPredicates();
    }

    public ObjectCondition(String attribute, AttributeType attributeType, List<BooleanPredicate> booleanPredicates){
        this.attribute = attribute;
        this.type = attributeType;
        this.booleanPredicates = new ArrayList<BooleanPredicate>(booleanPredicates.size());
        for(BooleanPredicate bp: booleanPredicates){
            this.booleanPredicates.add(new BooleanPredicate(bp));
        }
    }

    public ObjectCondition(String policy_id, String attribute, AttributeType attributeType){
        this.policy_id = policy_id;
        this.attribute = attribute;
        this.type = attributeType;
        this.booleanPredicates = new ArrayList<>();
    }

    public ObjectCondition(String policy_id, String attribute, AttributeType attributeType, String v1, Operation o1) {
        this.policy_id = policy_id;
        this.attribute = attribute;
        this.type = attributeType;
        List<BooleanPredicate> booleanPredicates = new ArrayList<>();
        booleanPredicates.add(new BooleanPredicate(o1, v1));
        booleanPredicates.add(new BooleanPredicate(o1, v1));
        this.booleanPredicates = booleanPredicates;
    }


    public ObjectCondition(String policy_id, String attribute, AttributeType attributeType, String v1,
                           Operation o1, String v2, Operation o2){
        this.policy_id = policy_id;
        this.attribute = attribute;
        this.type = attributeType;
        List<BooleanPredicate> booleanPredicates = new ArrayList<>();
        booleanPredicates.add(new BooleanPredicate(o1, v1));
        booleanPredicates.add(new BooleanPredicate(o2, v2));
        this.booleanPredicates = booleanPredicates;
    }

    /**
     * For attribute type of INTEGER and histogram type of singleton
     * e.g., temperature or energy
     * @return
     */
    private double singletonRange(){
        double frequency = 0.0001;
        if(this.getType() == AttributeType.INTEGER) {
            for (int i = 0; i < Histogram.getInstance().getBucketMap().get(this.getAttribute()).size(); i++) {
                Bucket b = Histogram.getInstance().getBucketMap().get(this.getAttribute()).get(i);
                if (Integer.parseInt(b.getValue()) >= Integer.parseInt(this.getBooleanPredicates().get(0).getValue())
                        && Integer.parseInt(b.getValue()) <= Integer.parseInt(this.getBooleanPredicates().get(1).getValue())) {
                    frequency += b.getFreq();
                }
            }
        }
        else if(this.getType() == AttributeType.DATE) {
            DateFormat formatter = new SimpleDateFormat(PolicyConstants.DATE_FORMAT);
            try {
                List<Bucket> buckets = Histogram.getInstance().getBucketMap().get(this.getAttribute());
                for (Bucket b:buckets) {
                    if (formatter.parse(b.getValue()).compareTo(formatter.parse(this.getBooleanPredicates().get(0).getValue())) >= 0
                            && formatter.parse(b.getValue()).compareTo(formatter.parse(this.getBooleanPredicates().get(1).getValue())) < 0) {
                        frequency += b.getFreq();
                    }
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return frequency/100;
    }

    /**
     * For attribute type of String/Date and histogram type of singleton
     * e.g., location_id, start_date, user_profile, user_group
     * @return
     */
    private double singletonEquality(){
        double frequency = 0.0001;
        Bucket bucket = null;
        if(this.getType() == AttributeType.STRING) {
            bucket = Histogram.getInstance().getBucketMap().get(this.getAttribute()).stream()
                    .filter(b -> b.getValue().equalsIgnoreCase(this.getBooleanPredicates().get(0).getValue()))
                    .findFirst()
                    .orElse(null);
        }
        if (bucket != null) frequency += bucket.getFreq();
        return frequency/100;
    }

    /**
     * For attribute type of INTEGER and histogram type of equi-height
     * e.g., user_id or O_CUSTKEY
     * @return estimated selectivity
     */
    private double equiheightEquality() {
        double frequency = 0.0001;
        List<Bucket> buckets = Histogram.getInstance().getBucketMap().get(this.getAttribute());
        for (Bucket b : buckets) {
            if (Integer.parseInt(b.getLower()) <= Integer.parseInt(this.getBooleanPredicates().get(1).getValue())
                    && Integer.parseInt(b.getUpper()) >= Integer.parseInt(this.getBooleanPredicates().get(0).getValue())) {
                frequency += b.getFreq() / b.getNumberOfItems();
                break;
            }
        }
        return frequency / 100;
    }

    private double dateEquiheightRange() {
        double frequency = 0.0001;
        List<Bucket> buckets = Histogram.getInstance().getBucketMap().get(this.attribute);
        Bucket lKey = new Bucket();
        lKey.setAttribute(PolicyConstants.ORDER_DATE);
        lKey.setLower(this.getBooleanPredicates().get(0).getValue());
        Bucket uKey = new Bucket();
        uKey.setAttribute(PolicyConstants.ORDER_DATE);
        uKey.setLower(this.getBooleanPredicates().get(1).getValue());
        int lIndex = Collections.binarySearch(buckets, lKey);
        if (lIndex < 0) { // no exact match
            if (-lIndex > 2) {
                lIndex = -lIndex - 2;
            } else {//first bucket
                lIndex = -lIndex - 1;
            }
        }
        int uIndex = Collections.binarySearch(buckets, uKey);
        if (uIndex < 0) { // no exact match
            if (-uIndex > 2) {
                uIndex = -uIndex - 2;
            } else {//first bucket
                uIndex = -uIndex - 1;
            }
        }
        if (lIndex > buckets.size() - 1) return frequency;
        if (uIndex > buckets.size() - 1) uIndex = buckets.size() - 1;
//        for (int i = lIndex; i <= uIndex; i++) {
//            frequency += (buckets.get(i).getFreq()/buckets.get(i).getNumberOfItems());
//        }
//        return frequency/100;
        int indDiff = (uIndex - lIndex) == 0 ? 1 : uIndex - lIndex;
        return (indDiff) / (double) buckets.size();
    }

    /**
     * For attribute type of DOUBLE and histogram type of equi-height
     * e.g., O_TOTALPRICE
     * @return estimated selectivity
     */
    private double doubleEquiheightRange() {
        double frequency = 0.0001;
        List<Bucket> buckets = Histogram.getInstance().getBucketMap().get(this.attribute);
        Bucket lKey = new Bucket();
        lKey.setAttribute(PolicyConstants.ORDER_TOTAL_PRICE);
        lKey.setLower(this.getBooleanPredicates().get(0).getValue());
        Bucket uKey = new Bucket();
        uKey.setAttribute(PolicyConstants.ORDER_TOTAL_PRICE);
        uKey.setLower(this.getBooleanPredicates().get(1).getValue());
        int lIndex = Collections.binarySearch(buckets, lKey);
        if (lIndex < 0) { // no exact match
            if (-lIndex > 2) {
                lIndex = -lIndex - 2;
            } else {//first bucket
                lIndex = -lIndex - 1;
            }
        }
        int uIndex = Collections.binarySearch(buckets, uKey);
        if (uIndex < 0) { // no exact match
            if (-uIndex > 2) {
                uIndex = -uIndex - 2;
            } else {//first bucket
                uIndex = -uIndex - 1;
            }
        }
        if (lIndex > buckets.size() - 1) return frequency;
        if (uIndex > buckets.size() - 1) uIndex = buckets.size() - 1;
//        for (int i = lIndex; i <= uIndex; i++) {
//            frequency += (buckets.get(i).getFreq()/buckets.get(i).getNumberOfItems());
//        }
//        return frequency/100;
        int indDiff = (uIndex - lIndex) == 0 ? 1 : uIndex - lIndex;
        return (indDiff) / (double) buckets.size();
    }

        /**
         * For attribute type of TIME and histogram type of equi-height
         * e.g. start_time
         * @return
         */
    //TODO: Overestimates the selectivity as the partially contained buckets are completely counted
    private double timeEquiheightRange(){
        double frequency = 0.0001;
        List<Bucket> buckets = Histogram.getInstance().getBucketMap().get(this.attribute);
        Bucket lKey = new Bucket();
        lKey.setAttribute(PolicyConstants.START_TIME);
        lKey.setLower(this.getBooleanPredicates().get(0).getValue());
        Bucket uKey = new Bucket();
        uKey.setAttribute(PolicyConstants.START_TIME);
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
//        for (int i = lIndex; i <= uIndex; i++) {
//            frequency += (buckets.get(i).getFreq()/buckets.get(i).getNumberOfItems());
//        }
//        return frequency/100;
        int indDiff = (uIndex - lIndex) == 0 ? 1 : uIndex - lIndex;
        return (indDiff)/(double) buckets.size();
    }


    public double computeL(){
        if (Stream.of(PolicyConstants.LOCATIONID_ATTR, PolicyConstants.GROUP_ATTR, PolicyConstants.PROFILE_ATTR,
                PolicyConstants.ORDER_PRIORITY, PolicyConstants.ORDER_CLERK, PolicyConstants.ORDER_PROFILE,
                PolicyConstants.M_SHOP_NAME, PolicyConstants.M_INTEREST)
                .anyMatch(this.getAttribute()::equalsIgnoreCase)){
            return singletonEquality();
        }
        else if (this.getAttribute().equalsIgnoreCase(PolicyConstants.START_DATE)||
                this.getAttribute().equalsIgnoreCase(PolicyConstants.M_DATE)) {
            return singletonRange();
        }
        else if (this.getAttribute().equalsIgnoreCase(PolicyConstants.USERID_ATTR) ||
                this.getAttribute().equalsIgnoreCase(PolicyConstants.ORDER_CUSTOMER_KEY) ||
                this.getAttribute().equalsIgnoreCase(PolicyConstants.M_DEVICE)){
           return equiheightEquality();
        }
        else if (this.getAttribute().equalsIgnoreCase(PolicyConstants.START_TIME) ||
                this.getAttribute().equalsIgnoreCase(PolicyConstants.M_TIME)){
           return timeEquiheightRange();
        }
        else if (this.getAttribute().equalsIgnoreCase(PolicyConstants.ORDER_TOTAL_PRICE)) {
            return doubleEquiheightRange();
        }
        else if (this.getAttribute().equalsIgnoreCase(PolicyConstants.ORDER_DATE)){
            return dateEquiheightRange();
        }
        else {
            throw new PolicyEngineException("Unknown attribute");
        }
    }

    @Override
    public String toString() {
        return  "policyID= " + policy_id +
                " attribute='" + attribute +
                ", booleanPredicates=" + booleanPredicates +
                '}';
    }

    public boolean overlaps(ObjectCondition o2) {
        if (this.getType() == AttributeType.INTEGER) {
            int start1 = Integer.parseInt(this.getBooleanPredicates().get(0).getValue());
            int end1 = Integer.parseInt(this.getBooleanPredicates().get(1).getValue());
            int start2 = Integer.parseInt(o2.getBooleanPredicates().get(0).getValue());
            int end2 = Integer.parseInt(o2.getBooleanPredicates().get(1).getValue());
            return start1 <= end2 && end1 >= start2;
        } else if (this.getType() == AttributeType.DATE) {
            LocalDate start1 = LocalDate.parse(this.getBooleanPredicates().get(0).getValue());
            LocalDate end1 = LocalDate.parse(this.getBooleanPredicates().get(1).getValue());
            LocalDate start2 = LocalDate.parse(o2.getBooleanPredicates().get(0).getValue());
            LocalDate end2 = LocalDate.parse(o2.getBooleanPredicates().get(1).getValue());
            return (start1.isBefore(end2) && end1.isAfter(start2));
        } else if (this.getType() == AttributeType.TIME) {
            LocalTime start1 = LocalTime.parse(this.getBooleanPredicates().get(0).getValue());
            LocalTime start2 = LocalTime.parse(this.getBooleanPredicates().get(1).getValue());
            LocalTime end1 = LocalTime.parse(o2.getBooleanPredicates().get(0).getValue());
            LocalTime end2 = LocalTime.parse(o2.getBooleanPredicates().get(1).getValue());
            return (start1.isBefore(end2) && end1.isAfter(start2));
        } else if (this.getType() == AttributeType.DOUBLE) {
            double start1 = Double.parseDouble(this.getBooleanPredicates().get(0).getValue());
            double end1 = Double.parseDouble(this.getBooleanPredicates().get(1).getValue());
            double start2 = Double.parseDouble(o2.getBooleanPredicates().get(0).getValue());
            double end2 = Double.parseDouble(o2.getBooleanPredicates().get(1).getValue());
            return start1 <= end2 && end1 >= start2;
        } else {
            throw new PolicyEngineException("Incompatible Attribute Type");
        }
    }

    /**
     * Unions two overlapping predicates by extending ranges
     * the two object condtions unioned are separated by :
     * @param objectCondition
     * @return
     */
    public ObjectCondition union(ObjectCondition objectCondition){
        ObjectCondition extended = new ObjectCondition(this.policy_id + ":" +  objectCondition.getPolicy_id(), this.getAttribute(), this.getType());
        String begValue = this.getBooleanPredicates().get(0).getValue()
                .compareTo(objectCondition.getBooleanPredicates().get(0).getValue()) < 0  ?
                this.getBooleanPredicates().get(0).getValue(): objectCondition.getBooleanPredicates().get(0).getValue();
        String endValue = this.getBooleanPredicates().get(1).getValue()
                .compareTo(objectCondition.getBooleanPredicates().get(1).getValue()) > 0  ?
                this.getBooleanPredicates().get(1).getValue(): objectCondition.getBooleanPredicates().get(1).getValue();
        BooleanPredicate bp1 = new BooleanPredicate();
        bp1.setValue(begValue);
        bp1.setOperator(Operation.GTE);
        BooleanPredicate bp2 = new BooleanPredicate();
        bp2.setValue(endValue);
        bp2.setOperator(Operation.LTE);
        extended.getBooleanPredicates().add(bp1);
        extended.getBooleanPredicates().add(bp2);
        return extended;
    }


    /**
     * Intersects two overlapping predicates by extending ranges
     * the two object condtions intersected are separated by -
     * @param objectCondition
     * @return
     */
    public ObjectCondition intersect(ObjectCondition objectCondition){
        ObjectCondition extended = new ObjectCondition(this.policy_id + ":" + objectCondition.getPolicy_id(), this.getAttribute(), this.getType());
        String begValue = this.getBooleanPredicates().get(0).getValue()
                .compareTo(objectCondition.getBooleanPredicates().get(0).getValue()) > 0  ?
                this.getBooleanPredicates().get(0).getValue(): objectCondition.getBooleanPredicates().get(0).getValue();
        String endValue = this.getBooleanPredicates().get(1).getValue()
                .compareTo(objectCondition.getBooleanPredicates().get(1).getValue()) < 0  ?
                this.getBooleanPredicates().get(1).getValue(): objectCondition.getBooleanPredicates().get(1).getValue();
        BooleanPredicate bp1 = new BooleanPredicate();
        bp1.setValue(begValue);
        bp1.setOperator(Operation.GTE);
        BooleanPredicate bp2 = new BooleanPredicate();
        bp2.setValue(endValue);
        bp2.setOperator(Operation.LTE);
        extended.getBooleanPredicates().add(bp1);
        extended.getBooleanPredicates().add(bp2);
        return extended;
    }
}
