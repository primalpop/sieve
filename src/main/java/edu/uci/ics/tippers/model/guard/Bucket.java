package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.common.PolicyConstants;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Bucket implements Comparable<Bucket> {

    private String attribute;

    private String value;

    private String lower;

    private String upper;

    private double cumulfreq;

    private double freq;

    private int numberOfItems;

    public Bucket(String value, double cumulfreq, double freq) {
        this.value = value;
        this.cumulfreq = cumulfreq;
        this.freq = freq;
    }

    public Bucket(String lower, String upper, double cumulfreq, int numberOfItems) {
        this.lower = lower;
        this.upper = upper;
        this.cumulfreq = cumulfreq;
        this.numberOfItems = numberOfItems;
    }

    public Bucket() {

    }

    public String getAttribute() {
        return attribute;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getLower() {
        return lower;
    }

    public void setLower(String lower) {
        this.lower = lower;
    }

    public String getUpper() {
        return upper;
    }

    public void setUpper(String upper) {
        this.upper = upper;
    }

    public double getCumulfreq() {
        return cumulfreq;
    }

    public void setCumulfreq(double cumulfreq) {
        this.cumulfreq = cumulfreq;
    }

    public double getFreq() {
        return freq;
    }

    public void setFreq(double freq) {
        this.freq = freq;
    }

    public int getNumberOfItems() {
        return numberOfItems;
    }

    public void setNumberOfItems(int numberOfItems) {
        this.numberOfItems = numberOfItems;
    }

    public String toStringSingleton() {
        return "Bucket{" +
                "attribute = " + attribute +
                ", value='" + value + '\'' +
                ", cumulfreq=" + cumulfreq +
                ", freq=" + freq +
                '}';
    }


    public String toStringEH() {
        return "Bucket{" +
                "lower='" + lower + '\'' +
                ", upper='" + upper + '\'' +
                ", freq=" + freq +
                ", cumulfreq=" + cumulfreq +
                ", numberOfItems=" + numberOfItems +
                '}';
    }

    @Override
    public int compareTo(Bucket bucket) {
        if(this.getAttribute().equalsIgnoreCase(PolicyConstants.USERID_ATTR)){
            return Integer.parseInt(this.getLower()) - Integer.parseInt((bucket.getLower()));
        }
        else if(this.getAttribute().equalsIgnoreCase(PolicyConstants.START_DATE)) {
            DateFormat formatter = new SimpleDateFormat(PolicyConstants.DATE_FORMAT);
            try {
                return formatter.parse(this.getValue()).compareTo((formatter.parse(bucket.getValue())));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        else if(this.getAttribute().equalsIgnoreCase(PolicyConstants.ORDER_DATE)) {
            DateFormat formatter = new SimpleDateFormat(PolicyConstants.DATE_FORMAT);
            try {
                return formatter.parse(this.getLower()).compareTo((formatter.parse(bucket.getLower())));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        else if(this.getAttribute().equalsIgnoreCase(PolicyConstants.START_TIME)){
            DateFormat formatter = new SimpleDateFormat(PolicyConstants.TIME_FORMAT);
            try {
                return formatter.parse(this.getLower()).compareTo((formatter.parse(bucket.getLower())));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        else if(this.getAttribute().equalsIgnoreCase(PolicyConstants.ORDER_TOTAL_PRICE)){
            return (int) (Double.parseDouble(this.getLower()) - Double.parseDouble((bucket.getLower())));
        }
        else if (this.getAttribute().equalsIgnoreCase(PolicyConstants.LOCATIONID_ATTR)
                || this.getAttribute().equalsIgnoreCase(PolicyConstants.PROFILE_ATTR)
                || this.getAttribute().equalsIgnoreCase(PolicyConstants.GROUP_ATTR)) {
            return this.getValue().compareTo(bucket.getValue());
        }
        return 0;
    }



}
