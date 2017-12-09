package edu.uci.ics.tippers.model.tippers;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by cygnus on 12/7/17.
 */
public class TPayload {

    public TPayload(String location) {

        this.location = location;
    }

    @JsonProperty("location")
    String location;

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }
}
