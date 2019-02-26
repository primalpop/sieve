package edu.uci.ics.tippers.model.data;

import edu.uci.ics.tippers.db.MySQLConnectionManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by cygnus on 7/7/17.
 */
public class User {

    int user_id;

    String name;

    String email;

    String office;

    List<Integer> groups;

    private static Connection connection = MySQLConnectionManager.getInstance().getConnection();


    public User(int user_id, String name, List<Integer> groups) {
        this.user_id = user_id;
        this.name = name;
        this.groups = groups;
    }

    public User(int user_id, String name, String email, String office, List<Integer> groups) {
        this.user_id = user_id;
        this.name = name;
        this.email = email;
        this.office = office;
        this.groups = groups;
    }

    public User(int user_id) {
        this.user_id = user_id;
    }

    public User() {

    }

    public int getUser_id() {
        return user_id;
    }

    public void setUser_id(int user_id) {
        this.user_id = user_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Integer> getGroups() {
        return groups;
    }

    public void setGroups(List<Integer> groups) {
        this.groups = groups;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getOffice() {
        return office;
    }

    public void setOffice(String office) {
        this.office = office;
    }

    public List<UserGroup> getUserGroups(){
        List<UserGroup> userGroups = new ArrayList<>();
        PreparedStatement queryStm = null;
        try {
            queryStm = connection.prepareStatement("SELECT ug.id, ug.description, ug.name, ug.owner " +
                    "FROM USER_GROUP as ug, USER_GROUP_MEMBERSHIP as ugm where ugm.USER_ID = ? " +
                    " AND ugm.USER_GROUP_ID = ug.id");

            queryStm.setInt(1, this.getUser_id());
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) {
               UserGroup ug = new UserGroup();
               ug.setGroup_id(Integer.parseInt(rs.getString("ug.id")));
               ug.setDescription(rs.getString("ug.description"));
               ug.setName(rs.getString("ug.name"));
               ug.setOwner(new User(Integer.parseInt(rs.getString("ug.owner"))));
               userGroups.add(ug);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return userGroups;
    }


}
