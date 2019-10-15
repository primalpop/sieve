package edu.uci.ics.tippers.model.data;

import java.util.Objects;

/**
 * Created by cygnus on 7/5/17.
 */
public class LongPresence {

    private String id;

    private String user_id;

    private String location;

    private String timeStamp;

    private String temperature;

    private String energy;

    private String activity;

    public LongPresence(){

    }

    public LongPresence(String user_id, String location, String timeStamp, String temperature, String energy, String activity) {
        this.user_id = user_id;
        this.location = location;
        this.timeStamp = timeStamp;
        this.temperature = temperature;
        this.energy = energy;
        this.activity = activity;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getTemperature() {
        return temperature;
    }

    public void setTemperature(String temperature) {
        this.temperature = temperature;
    }

    public String getEnergy() {
        return energy;
    }

    public void setEnergy(String energy) {
        this.energy = energy;
    }

    public String getActivity() {
        return activity;
    }

    public void setActivity(String activity) {
        this.activity = activity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LongPresence that = (LongPresence) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id);
    }
}
