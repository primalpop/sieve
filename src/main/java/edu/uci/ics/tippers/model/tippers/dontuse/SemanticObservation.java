package edu.uci.ics.tippers.model.tippers.dontuse;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.sql.Timestamp;

/**
 * Created by cygnus on 7/5/17.
 */
public class SemanticObservation {

    @JsonProperty("id")
    int id;

    @JsonProperty("semantic_entity_id")
    int semantic_entity_id;

    @JsonProperty("payload")
    String payload;

    @JsonProperty("confidence")
    float confidence;

    @JsonProperty("timestamp")
    Timestamp timeStamp;

    @JsonProperty("so_type_id")
    int so_type_id;

    @JsonProperty("virtual_sensor_id")
    int virtual_sensor_id;

    public SemanticObservation(int id, int semantic_entity_id, String payload, float confidence, Timestamp timeStamp, int so_type_id, int virtual_sensor_id) {
        this.id = id;
        this.semantic_entity_id = semantic_entity_id;
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

    public int getSemantic_entity_id() {
        return semantic_entity_id;
    }

    public void setSemantic_entity_id(int semantic_entity_id) {
        this.semantic_entity_id = semantic_entity_id;
    }

    public String getPayload() {
        if(this.payload.length() == 18)
            return payload.substring(13, 17);
        else
            return payload.substring(13, 19);
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public float getConfidence() {
        return confidence;
    }

    public void setConfidence(float confidence) {
        this.confidence = confidence;
    }

    public Timestamp getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Timestamp timeStamp) {
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
                ", semantic_entity_id=" + semantic_entity_id +
                ", payload=" + payload +
                ", confidence=" + confidence +
                ", timeStamp=" + timeStamp +
                ", so_type_id=" + so_type_id +
                ", virtual_sensor_id=" + virtual_sensor_id +
                '}';
    }
}
