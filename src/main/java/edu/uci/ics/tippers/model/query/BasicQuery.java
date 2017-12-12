package edu.uci.ics.tippers.model.query;

import java.sql.Timestamp;

/**
 * Created by cygnus on 7/7/17.
 *
 * Includes Q1 to Q5 from experiments
 */
public class BasicQuery {

    private String user_id;

    private String location_id;

    private Timestamp timestamp;

    private float temperature;

    private float wemo;

    public BasicQuery(String user_id, String location_id, Timestamp timestamp, float temperature, float wemo) {
        this.user_id = user_id;
        this.location_id = location_id;
        this.timestamp = timestamp;
        this.temperature = temperature;
        this.wemo = wemo;
    }

    public BasicQuery(){

    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getLocation_id() {
        return location_id;
    }

    public void setLocation_id(String location_id) {
        this.location_id = location_id;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }
}
