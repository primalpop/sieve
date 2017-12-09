package edu.uci.ics.tippers.model.tippers;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Calendar;

/**
 * Created by cygnus on 7/5/17.
 */
public class Semantic_Observation {

    @JsonProperty("id")
    int id;

    @JsonProperty("semantic_entity_id")
    int user;

    @JsonProperty("payload")
    TPayload location;

    @JsonProperty("confidence")
    float confidence;

    @JsonProperty("timeStamp")
    Calendar timeStamp;

    @JsonProperty("so_type_id")
    int so_type_id;

    @JsonProperty("virtual_sensor_id")
    int virtual_sensor_id;

    public Semantic_Observation(int id, int user, TPayload location, float confidence, Calendar timeStamp, int so_type_id, int virtual_sensor_id) {
        this.id = id;
        this.user = user;
        this.location = location;
        this.confidence = confidence;
        this.timeStamp = timeStamp;
        this.so_type_id = so_type_id;
        this.virtual_sensor_id = virtual_sensor_id;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getUser() {
        return user;
    }

    public void setUser(int user) {
        this.user = user;
    }

    public TPayload getLocation() {
        return location;
    }

    public void setLocation(TPayload location) {
        this.location = location;
    }

    public float getConfidence() {
        return confidence;
    }

    public void setConfidence(float confidence) {
        this.confidence = confidence;
    }

    public Calendar getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Calendar timeStamp) {
        this.timeStamp = timeStamp;
    }

    public int getSo_type_id() {
        return so_type_id;
    }

    public void setSo_type_id(int so_type_id) {
        this.so_type_id = so_type_id;
    }

    public int getVirtual_sensor_id() {
        return virtual_sensor_id;
    }

    public void setVirtual_sensor_id(int virtual_sensor_id) {
        this.virtual_sensor_id = virtual_sensor_id;
    }
}
