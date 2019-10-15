package edu.uci.ics.tippers.model.data;

import java.sql.Timestamp;

public class Presence {

    int id;

    String user_id;

    String location_id;

    Timestamp start;

    Timestamp finish;

    public Presence(){

    }

    public Presence(String user_id, String location_id, Timestamp start, Timestamp finish) {
        this.user_id = user_id;
        this.location_id = location_id;
        this.start = start;
        this.finish = finish;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
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

    public Timestamp getStart() {
        return start;
    }

    public void setStart(Timestamp start) {
        this.start = start;
    }

    public Timestamp getFinish() {
        return finish;
    }

    public void setFinish(Timestamp finish) {
        this.finish = finish;
    }
}
