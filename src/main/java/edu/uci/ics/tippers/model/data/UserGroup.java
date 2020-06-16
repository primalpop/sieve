package edu.uci.ics.tippers.model.data;

import edu.uci.ics.tippers.dbms.mysql.MySQLConnectionManager;

import java.sql.*;
import java.util.List;

/**
 * Created by cygnus on 12/7/17.
 */
public class UserGroup {

    private String name;

    private User owner;

    private List<User> members;

    private static Connection connection = MySQLConnectionManager.getInstance().getConnection();

    public UserGroup(String name, List<User> members) {
        this.name = name;
        this.members = members;
    }

    public UserGroup(String name, User owner, List<User> members) {
        this.name = name;
        this.owner = owner;
        this.members = members;
    }

    public UserGroup() {

    }

    public UserGroup(String name) {
        this.name = name;
    }

    public User getOwner() {
        return owner;
    }

    public void setOwner(User owner) {
        this.owner = owner;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<User> getMembers() {
        if(members == null || members.isEmpty()) {
            retrieveMembers();
        }
        return members;
    }


    public void retrieveMembers(){
        PreparedStatement queryStm = null;
        try {
            queryStm = connection.prepareStatement("SELECT ugm.USER_ID as user " +
                    "FROM USER_GROUP_MEMBERSHIP as ugm where ugm.USER_GROUP_ID = ?" );
            queryStm.setString(1, this.getName());
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) {
                User user = new User();
                user.setUserId(Integer.parseInt(rs.getString("ugm.user")));
                this.getMembers().add(user);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}

