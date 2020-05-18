package edu.uci.ics.tippers.generation.policy.tpch;

public class PricePredicate {

    double low_range;
    double high_range;

    public PricePredicate(double low_range, double high_range){
        this.low_range = low_range;
        this.high_range = high_range;
    }

    public double getLow_range() {
        return low_range;
    }

    public void setLow_range(double low_range) {
        this.low_range = low_range;
    }

    public double getHigh_range() {
        return high_range;
    }

    public void setHigh_range(double high_range) {
        this.high_range = high_range;
    }
}
