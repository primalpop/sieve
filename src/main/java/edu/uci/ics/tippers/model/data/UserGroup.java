package edu.uci.ics.tippers.model.data;

import java.util.List;

/**
 * Created by cygnus on 12/7/17.
 */
public class UserGroup {

    private int group_id;

    private String name;

    private User owner;

    private String description;

    private List<Integer> users;


    public UserGroup(int group_id, String name, List<Integer> users) {
        this.group_id = group_id;
        this.name = name;
        this.users = users;
    }

    public UserGroup(int group_id, String name, User owner, String description, List<Integer> users) {
        this.group_id = group_id;
        this.name = name;
        this.owner = owner;
        this.description = description;
        this.users = users;
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

    public List<Integer> getUsers() {
        return users;
    }

    public void setUsers(List<Integer> users) {
        this.users = users;
    }
}
