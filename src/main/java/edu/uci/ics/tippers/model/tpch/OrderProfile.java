package edu.uci.ics.tippers.model.tpch;

public enum OrderProfile {
    LOW ("5-LOW") ,
    URGENT ("1-URGENT"),
    NOT_SPECIFIED ("4-NOT SPECIFIED"),
    HIGH ("2-HIGH"),
    MEDIUM ("3-MEDIUM");

    private final String priority;

    private OrderProfile(String profile) {
        this.priority = profile;
    }

    public String getPriority() {
        return priority;
    }
}
