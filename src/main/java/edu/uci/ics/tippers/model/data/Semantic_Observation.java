package edu.uci.ics.tippers.model.data;

/**
 * Created by cygnus on 7/5/17.
 */
public class Semantic_Observation {

    int id;

    int user_id;

    int location;

    String timeStamp;

    float temperature;

    float wemo;

    public Semantic_Observation(){

    }

    public Semantic_Observation(int id, int user_id, int location, String timeStamp) {
        this.id = id;
        this.user_id = user_id;
        this.location = location;
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "Semantic_Observation{" +
                "id=" + id +
                ", user_id=" + user_id +
                ", location=" + location +
                ", timeStamp='" + timeStamp + '\'' +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getUser_id() {
        return user_id;
    }

    public void setUser_id(int user_id) {
        this.user_id = user_id;
    }

    public int getLocation() {
        return location;
    }

    public void setLocation(int location) {
        this.location = location;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }
}
