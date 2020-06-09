package edu.uci.ics.tippers.execution;

public class ExpResult {

    int numberOfPolicies;
    long baselineR;
    long sieveR;
    int numberOfGuards;

    public int getNumberOfPolicies() {
        return numberOfPolicies;
    }

    public void setNumberOfPolicies(int numberOfPolicies) {
        this.numberOfPolicies = numberOfPolicies;
    }

    public long getBaselineR() {
        return baselineR;
    }

    public void setBaselineR(long baselineR) {
        this.baselineR = baselineR;
    }

    public long getSieveR() {
        return sieveR;
    }

    public void setSieveR(long sieveR) {
        this.sieveR = sieveR;
    }

    public int getNumberOfGuards() {
        return numberOfGuards;
    }

    public void setNumberOfGuards(int numberOfGuards) {
        this.numberOfGuards = numberOfGuards;
    }
}
