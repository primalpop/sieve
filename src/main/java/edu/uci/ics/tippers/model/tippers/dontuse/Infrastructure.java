package edu.uci.ics.tippers.model.tippers.dontuse;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by cygnus on 7/7/17.
 */
public class Infrastructure {

    @JsonProperty("SEMANTIC_ENTITY_ID")
    int location_id;

    @JsonProperty("name")
    String name;

    @JsonProperty("type")
    String type;

    @JsonProperty("REGION_ID")
    int region_id;

    @JsonProperty("REGION_NAME")
    String region_name;

    @JsonProperty("capacity")
    int capacity;

    public int getLocation_id() {
        return location_id;
    }

    public void setLocation_id(int location_id) {
        this.location_id = location_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getRegion_id() {
        return region_id;
    }

    public void setRegion_id(int region_id) {
        this.region_id = region_id;
    }

    public String getRegion_name() {
        return region_name;
    }

    public void setRegion_name(String region_name) {
        this.region_name = region_name;
    }

    public int getCapacity() {
        return capacity;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    public Infrastructure(int location_id, String name, String type, int region_id, String region_name, int capacity) {

        this.location_id = location_id;
        this.name = name;
        this.type = type;
        this.region_id = region_id;
        this.region_name = region_name;
        this.capacity = capacity;
    }

    public Infrastructure(){

    }
}
