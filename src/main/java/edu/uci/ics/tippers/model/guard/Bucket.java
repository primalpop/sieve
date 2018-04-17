package edu.uci.ics.tippers.model.guard;

public class Bucket {

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

    @Override
    public String toString() {
        return "Bucket{" +
                "value='" + value + '\'' +
                ", cumulfreq=" + cumulfreq +
                ", freq=" + freq +
                '}';
    }
}
