package edu.uci.ics.tippers.model.query;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Created by cygnus on 7/7/17.
 *
 * */
public class BasicQuery {

    private String user_id;

    private String location_id;

    private Timestamp timestamp;

    private String temperature;

    private String wemo;

    private String activity;

    public BasicQuery(String user_id, String location_id, Timestamp timestamp, String temperature, String wemo, String activity) {
        this.user_id = user_id;
        this.location_id = location_id;
        this.timestamp = timestamp;
        this.temperature = temperature;
        this.wemo = wemo;
        this.activity = activity;
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

    public String getTemperature() {
        return temperature;
    }

    public void setTemperature(String temperature) {
        this.temperature = temperature;
    }

    public String getWemo() {
        return wemo;
    }

    public void setWemo(String wemo) {
        this.wemo = wemo;
    }

    public String getActivity() {
        return activity;
    }

    public void setActivity(String activity) {
        this.activity = activity;
    }

    @Override
    public String toString() {
        return "BasicQuery{" +
                "user_id='" + user_id + '\'' +
                ", location_id='" + location_id + '\'' +
                ", timestamp=" + timestamp +
                ", temperature='" + temperature + '\'' +
                ", wemo='" + wemo + '\'' +
                ", activity='" + activity + '\'' +
                '}';
    }


    public boolean checkAllNull(){
        return Stream.of(timestamp, wemo,
                temperature, user_id, location_id, activity)
                .allMatch(Objects::isNull);
    }

    public String createPredicate(){

        if(checkAllNull()) return "(user_id = 10000)";

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        StringBuilder sqlPredicate = new StringBuilder();
        sqlPredicate.append("(");
        if(user_id != null) {
            sqlPredicate.append("user_id = ");
            sqlPredicate.append("\'").append(user_id).append("\'");
        }
        if(location_id != null) {
            if(sqlPredicate.length() > 1) sqlPredicate.append(" AND ");
            sqlPredicate.append("location_id = ");
            sqlPredicate.append("\'").append(location_id).append("\'");
        }
        if(temperature != null) {
            if(sqlPredicate.length() > 1) sqlPredicate.append(" AND ");
            sqlPredicate.append("temperature = ");
            sqlPredicate.append("\'").append(temperature).append("\'");
        }
        if(wemo != null) {
            if(sqlPredicate.length() > 1) sqlPredicate.append(" AND ");
            sqlPredicate.append("energy = ");
            sqlPredicate.append("\'").append(wemo).append("\'");
        }
        if(activity != null) {
            if(sqlPredicate.length() > 1) sqlPredicate.append(" AND ");
            sqlPredicate.append("activity = ");
            sqlPredicate.append("\'").append(activity).append("\'");
        }
        if(timestamp != null) {
            if(sqlPredicate.length() > 1) sqlPredicate.append(" AND ");
            sqlPredicate.append("timeStamp = ");
            sqlPredicate.append("\'").append( sdf.format(timestamp)).append("\'");
        }
        sqlPredicate.append(")");
        return sqlPredicate.toString();
    }


}
