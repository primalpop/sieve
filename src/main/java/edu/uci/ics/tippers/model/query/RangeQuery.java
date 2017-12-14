package edu.uci.ics.tippers.model.query;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Author primpap
 */


public class RangeQuery {

    private Timestamp start_timestamp;

    private Timestamp end_timestamp;

    private String start_wemo;

    private String end_wemo;

    private String start_temp;

    private String end_temp;

    private String user_id;

    private String location_id;

    private String activity;

    public RangeQuery(Timestamp start_timestamp, Timestamp end_timestamp, String start_wemo, String end_wemo,
                      String start_temp, String end_temp, String user_id, String location_id, String activity) {
        this.start_timestamp = start_timestamp;
        this.end_timestamp = end_timestamp;
        this.start_wemo = start_wemo;
        this.end_wemo = end_wemo;
        this.start_temp = start_temp;
        this.end_temp = end_temp;
        this.user_id = user_id;
        this.location_id = location_id;
        this.activity = activity;
    }

    public RangeQuery(){

    }

    public Timestamp getStart_timestamp() {
        return start_timestamp;
    }

    public void setStart_timestamp(Timestamp start_timestamp) {
        this.start_timestamp = start_timestamp;
    }

    public Timestamp getEnd_timestamp() {
        return end_timestamp;
    }

    public void setEnd_timestamp(Timestamp end_timestamp) {
        this.end_timestamp = end_timestamp;
    }

    public String getStart_wemo() {
        return start_wemo;
    }

    public void setStart_wemo(String start_wemo) {
        this.start_wemo = start_wemo;
    }

    public String getEnd_wemo() {
        return end_wemo;
    }

    public void setEnd_wemo(String end_wemo) {
        this.end_wemo = end_wemo;
    }

    public String getStart_temp() {
        return start_temp;
    }

    public void setStart_temp(String start_temp) {
        this.start_temp = start_temp;
    }

    public String getEnd_temp() {
        return end_temp;
    }

    public void setEnd_temp(String end_temp) {
        this.end_temp = end_temp;
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

    public String getActivity() {
        return activity;
    }

    public void setActivity(String activity) {
        this.activity = activity;
    }


    public boolean checkAllNull(){
        return Stream.of(start_timestamp, end_timestamp, start_wemo, end_wemo,
                start_temp, end_temp, user_id, location_id, activity)
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
        if(activity != null) {
            if(sqlPredicate.length() > 1) sqlPredicate.append(" AND ");
            sqlPredicate.append("activity = ");
            sqlPredicate.append("\'").append(activity).append("\'");
        }
        if(start_timestamp != null) {
            if(sqlPredicate.length() > 1) sqlPredicate.append(" AND ");
            sqlPredicate.append("timeStamp >= ");
            sqlPredicate.append("\'").append( sdf.format(start_timestamp)).append("\'");
        }
        if(end_timestamp != null) {
            if(sqlPredicate.length() > 1) sqlPredicate.append(" AND ");
            sqlPredicate.append("timeStamp <= ");
            sqlPredicate.append("\'").append( sdf.format(end_timestamp)).append("\'");
        }
        if(start_wemo != null) {
            if(sqlPredicate.length() > 1) sqlPredicate.append(" AND ");
            sqlPredicate.append("energy >= ");
            sqlPredicate.append("\'").append(start_wemo).append("\'");
        }
        if(end_wemo != null) {
            if(sqlPredicate.length() > 1) sqlPredicate.append(" AND ");
            sqlPredicate.append("energy <= ");
            sqlPredicate.append("\'").append(end_wemo).append("\'");
        }
        if(start_temp != null) {
            if(sqlPredicate.length() > 1) sqlPredicate.append(" AND ");
            sqlPredicate.append("temperature >= ");
            sqlPredicate.append("\'").append(start_temp).append("\'");
        }
        if(end_temp != null) {
            if(sqlPredicate.length() > 1) sqlPredicate.append(" AND ");
            sqlPredicate.append("temperature <= ");
            sqlPredicate.append("\'").append(end_temp).append("\'");
        }
        sqlPredicate.append(")");
        return sqlPredicate.toString();
    }
}
