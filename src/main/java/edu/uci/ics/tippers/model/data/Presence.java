package edu.uci.ics.tippers.model.data;

import java.sql.Timestamp;
import java.util.Objects;

public class Presence {

    int id;

    User user;

    Location location;

    Timestamp start;

    Timestamp finish;

    public Presence(){
    }

    public Presence(User user, Location location, Timestamp start, Timestamp finish) {
        this.user = user;
        this.location = location;
        this.start = start;
        this.finish = finish;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public User getUser() {
        return user;
    }

    public void setUser_id(User user) {
        this.user = user;
    }

    public Location getLocation() {
        return location;
    }

    public void setLocation(Location location) {
        this.location = location;
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


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Presence presence = (Presence) o;
        return id == presence.id &&
                user.equals(presence.user) &&
                location.equals(presence.location) &&
                start.equals(presence.start) &&
                finish.equals(presence.finish);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, user, location, start, finish);
    }
}
