package edu.uci.ics.tippers.model.data;

import edu.uci.ics.tippers.db.MySQLConnectionManager;

import java.sql.*;
import java.util.List;

/**
 * Created by cygnus on 12/7/17.
 */
public class UserGroup {

    private int group_id;

    private String name;

    private User owner;

    private String description;

    private String region_name;

    private String floor;

    private List<User> members;

    private static Connection connection = MySQLConnectionManager.getInstance().getConnection();

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

    public String getRegion_name() {
        return region_name;
    }

    public void setRegion_name(String region_name) {
        this.region_name = region_name;
    }

    public String getFloor() {
        return floor;
    }

    public void setFloor(String floor) {
        this.floor = floor;
    }

    public List<User> getMembers() {
        if(members == null || members.isEmpty()) {
            retrieveMembers();
        }
        return members;
    }

    public void retrieveUserGroupDetails() {
        PreparedStatement queryStm = null;
        try {
            queryStm = connection.prepareStatement("SELECT ug.regionName, ug.floor " +
                    "FROM USER_GROUP as ug where ug.ID = ?" );
            queryStm.setString(1, String.valueOf(this.group_id));
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) {
                this.setRegion_name(rs.getString("ug.regionName"));
                this.setFloor(rs.getString("ug.floor"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void retrieveMembers(){
        PreparedStatement queryStm = null;
        try {
            queryStm = connection.prepareStatement("SELECT ugm.USER_ID as user " +
                    "FROM USER_GROUP_MEMBERSHIP as ugm where ugm.USER_GROUP_ID = ?" );
            queryStm.setString(1, String.valueOf(this.group_id));
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) {
                User user = new User();
                user.setUserId(Integer.parseInt(rs.getString("ugm.user")));
                user.retrieveUserDetails();
                this.getMembers().add(user);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}

