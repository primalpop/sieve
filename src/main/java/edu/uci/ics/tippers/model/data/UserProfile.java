package edu.uci.ics.tippers.model.data;

public enum UserProfile {

    GRADUATE ("graduate") ,
    FACULTY ("faculty"),
    STAFF ("staff"),
    UNDERGRAD ("undergrad"),
    VISITOR ("visitor");

    private final String profile;

    private UserProfile(String profile) {
        this.profile = profile;
    }

    public String getValue() {
        return profile;
    }
}
