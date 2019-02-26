package edu.uci.ics.tippers.model.data;

import edu.uci.ics.tippers.db.MySQLConnectionManager;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by cygnus on 12/7/17.
 */
public class UserGroup {

    private int group_id;

    private String name;

    private User owner;

    private String description;

    private List<TimePeriod> timePeriods;

    private String group_type;

    private String location;

    private List<User> members;

    private static Connection connection = MySQLConnectionManager.getInstance().getConnection();

    private static final long GROUP_MEMBERSHIP = 900000;

    public UserGroup(int group_id, String name, List<User> members) {
        this.group_id = group_id;
        this.name = name;
        this.members = members;
    }

    public UserGroup(int group_id, String name, User owner, String description, List<User> members) {
        this.group_id = group_id;
        this.name = name;
        this.owner = owner;
        this.description = description;
        this.members = members;
    }

    public UserGroup() {

    }

    public User getOwner() {
        return owner;
    }

    public void setOwner(User owner) {
        this.owner = owner;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int getGroup_id() {
        return group_id;
    }

    public void setGroup_id(int group_id) {
        this.group_id = group_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<TimePeriod> getTimePeriods() {
        return timePeriods;
    }

    public void setTimePeriods(List<TimePeriod> timePeriods) {
        this.timePeriods = timePeriods;
    }

    public String getGroup_type() {
        return group_type;
    }

    public void setGroup_type(String group_type) {
        this.group_type = group_type;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }


    private boolean satisfyMembership(TimePeriod tp, Timestamp start, Timestamp end){
        Timestamp sOverlap, eOverlap;
        if(tp.getStart().before(start))
            sOverlap = start;
        else sOverlap = tp.getStart();
        if(tp.getEnd().before(end))
            eOverlap = end;
        else eOverlap = tp.getEnd();
        return (eOverlap.getTime() - sOverlap.getTime()) > GROUP_MEMBERSHIP;
    }

    public void retrieveMembers(){
        List<User> members = new ArrayList<>();
        for (TimePeriod tp: this.getTimePeriods()) {
            PreparedStatement queryStm = null;
            try {
                queryStm = connection.prepareStatement("SELECT p.userId, p.startTimestamp, p.endTimestamp " +
                        "FROM PRESENCE AS p WHERE p.location = ? AND (p.startTimestamp <= ? AND p.endTimestamp >= ?)");

                queryStm.setString(1, this.getLocation());
                queryStm.setTimestamp(2, tp.getEnd());
                queryStm.setTimestamp(3, tp.getStart());
                ResultSet rs = queryStm.executeQuery();
                while (rs.next()) {
                    User user = retrieveUser(Integer.parseInt(rs.getString("p.userId")));
                    if (user != null){
                        Timestamp start = rs.getTimestamp("p.startTimestamp");
                        Timestamp end = rs.getTimestamp("p.endTimestamp");
                        if(satisfyMembership(tp, start, end)){
                            members.add(user);
                        }
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        this.members = members;
    }


    private User retrieveUser(int userId) {
        User user = new User();
        PreparedStatement queryStm = null;
        try {
            queryStm = connection.prepareStatement("SELECT u.name, u.email_id, u.SEMANTIC_ENTITY_ID " +
                    "FROM USER as u where u.SEMANTIC_ENTITY_ID = ?" );
            queryStm.setInt(1, userId);
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) {
                user.setName(rs.getString("u.name"));
                user.setEmail(rs.getString("u.email_id"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return user;
    }

    @Override
    public String toString() {
        return "UserGroup{" +
                "group_id=" + group_id +
                ", name='" + name + '\'' +
                ", owner=" + owner +
                ", description='" + description + '\'' +
                ", timePeriods=" + timePeriods +
                ", group_type='" + group_type + '\'' +
                ", location='" + location + '\'' +
                ", members=" + members +
                '}';
    }
}

