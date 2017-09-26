package model;

/**
 * Created by cygnus on 7/5/17.
 */
public class Presence {

    int id;

    int user_id;

    int location;

    String timeStamp;

    public Presence(){

    }

    public Presence(int id, int user_id, int location, String timeStamp) {
        this.id = id;
        this.user_id = user_id;
        this.location = location;
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "model.Presence{" +
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
