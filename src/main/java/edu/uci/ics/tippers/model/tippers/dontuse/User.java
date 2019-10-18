package edu.uci.ics.tippers.model.tippers.dontuse;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by cygnus on 7/7/17.
 */
public class User {

    public User(int user_id, String name, String office, String googleAuthToken, String email) {
        this.user_id = user_id;
        this.name = name;
        this.office = office;
        this.googleAuthToken = googleAuthToken;
        this.email = email;
    }

    @JsonProperty("SEMANTIC_ENTITY_ID")
    int user_id;

    @JsonProperty("name")
    String name;

    @JsonProperty("office")
    String office;

    @JsonProperty("googleAuthToken")
    String googleAuthToken;

    @JsonProperty("email")
    String email;


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

    public String getOffice() {
        return office;
    }

    public void setOffice(String office) {
        this.office = office;
    }

    public String getGoogleAuthToken() {
        return googleAuthToken;
    }

    public void setGoogleAuthToken(String googleAuthToken) {
        this.googleAuthToken = googleAuthToken;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public User(){

    }
}
