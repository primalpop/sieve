package edu.uci.ics.tippers.model.data;

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
}
