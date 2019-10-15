package edu.uci.ics.tippers.model.policy;

import com.fasterxml.jackson.annotation.JsonProperty;


public enum Operation {

    @JsonProperty("=")
    EQ("="),

    @JsonProperty(">=")
    GTE(">="),

    @JsonProperty("<=")
    LTE("<="),

    @JsonProperty(">")
    GT(">"),

    @JsonProperty("<")
    LT("<");

    private String value;
    private Operation(String value) {
       this.value = value;
    }
    public String toString() {
        return this.value;
    }

}
