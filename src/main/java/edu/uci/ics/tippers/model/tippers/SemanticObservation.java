package edu.uci.ics.tippers.model.tippers;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Calendar;

/**
 * Created by cygnus on 7/5/17.
 */
public class SemanticObservation {

    @JsonProperty("id")
    int id;

    @JsonProperty("semantic_entity_id")
    int user;

    @JsonProperty("payload")
    TPayload payload;

    @JsonProperty("confidence")
    float confidence;

    @JsonProperty("timeStamp")
    Calendar timeStamp;

    @JsonProperty("so_type_id")
    int so_type_id;

    @JsonProperty("virtual_sensor_id")
    int virtual_sensor_id;

    public SemanticObservation(int id, int user, TPayload payload, float confidence, Calendar timeStamp, int so_type_id, int virtual_sensor_id) {
        this.id = id;
        this.user = user;
        this.payload = payload;
        this.confidence = confidence;
        this.timeStamp = timeStamp;
        this.so_type_id = so_type_id;
        this.virtual_sensor_id = virtual_sensor_id;
    }

    public SemanticObservation(){

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

    public TPayload getPayload() {
        return payload;
    }

    public void setPayload(TPayload payload) {
        this.payload = payload;
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

    @Override
    public String toString() {
        return "SemanticObservation{" +
                "id=" + id +
                ", user=" + user +
                ", payload=" + payload +
                ", confidence=" + confidence +
                ", timeStamp=" + timeStamp +
                ", so_type_id=" + so_type_id +
                ", virtual_sensor_id=" + virtual_sensor_id +
                '}';
    }
}
