package edu.uci.ics.tippers.model.data;

import edu.uci.ics.tippers.dbms.mysql.MySQLConnectionManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by cygnus on 7/7/17.
 */
public class User {

    private int userId;
    private UserProfile profile;
    private List<UserGroup> groups;

    private static Connection connection = MySQLConnectionManager.getInstance().getConnection();

    public User(int userId, List<UserGroup> groups) {
        this.userId = userId;
        this.groups = groups;
    }

    public User(int userId) {
        this.userId = userId;
    }

    public User() {

    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public List<UserGroup> getGroups() {
        return groups;
    }

    public void setGroups(List<UserGroup> groups) {
        this.groups = groups;
    }

    public UserProfile getProfile() {
        return profile;
    }

    public void setProfile(UserProfile profile) {
        this.profile = profile;
    }

    @Override
    public int hashCode() {
        return this.userId;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof User))
            return false;
        return (this.userId == ((User) obj).userId);
    }

    public void retrieveUserGroups() {
        PreparedStatement queryStm = null;
        try {
            queryStm = connection.prepareStatement("SELECT USER_GROUP_ID as ug_id " +
                    "FROM USER_GROUP_MEMBERSHIP as ugm where ugm.USER_ID = ? ");
            queryStm.setInt(1, this.getUserId());
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) {
                UserGroup ug = new UserGroup();
                ug.setName(rs.getString("ug_id"));
                this.getGroups().add(ug);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
