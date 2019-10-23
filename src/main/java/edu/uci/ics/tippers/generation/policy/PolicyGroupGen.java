package edu.uci.ics.tippers.generation.policy;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.manager.PolicyPersistor;
import edu.uci.ics.tippers.model.data.UserProfile;
import edu.uci.ics.tippers.model.policy.BEPolicy;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;

/**
 * Generation of semi-realistic policies based on group memberships of a user
 */
public class PolicyGroupGen {

    private Connection connection;
    Random r;
    PolicyPersistor polper;
    PolicyGen pg;

    private HashMap<Integer, List<String>> user_groups;
    private HashMap<String, List<Integer>> group_members;
    private HashMap<Integer, List<String>> locClusters; //Forming random clusters of locations

    private final int ALL_GROUPS = 0;

    private int LARGE_GROUP_SIZE_THRESHOLD;
    private double PERCENTAGE_DEFAULT;
    private double PERCENTAGE_ALLOW;
    private int ACTIVE_CHOICES;
    private int START_WORKING_HOURS;
    private int DURATION_WORKING_HOURS;
    private int START_NIGHT_HOURS;
    private int DURATION_NIGHT_HOURS;
    private int START_ALL_HOURS;
    private int DURATION_ALL_HOURS;
    private double TIMESTAMP_CHANCE;

    public PolicyGroupGen(){
        this.connection = MySQLConnectionManager.getInstance().getConnection();
        r = new Random();
        polper = PolicyPersistor.getInstance();
        pg = new PolicyGen();
        user_groups = new HashMap<>();
        group_members = new HashMap<>();

        locClusters = new HashMap<>();
        for (int i = 0; i < 10 ; i++) {
            List<String> locations = new ArrayList<>();
            locClusters.put(i, locations);
        }
        for (String loc: pg.getAllLocations()) {
            int cluster = r.nextInt(10);
            locClusters.get(cluster).add(loc);
        }


        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("config/policygen.properties");
            Properties props = new Properties();
            if (inputStream != null) {
                props.load(inputStream);
                LARGE_GROUP_SIZE_THRESHOLD = Integer.parseInt(props.getProperty("group_size_limit"));
                PERCENTAGE_DEFAULT = Double.parseDouble(props.getProperty("default"));
                PERCENTAGE_ALLOW = Double.parseDouble(props.getProperty("allow"));
                ACTIVE_CHOICES = Integer.parseInt(props.getProperty("active_choices"));
                START_WORKING_HOURS = Integer.parseInt(props.getProperty("w_start"));
                DURATION_WORKING_HOURS = Integer.parseInt(props.getProperty("w_plus"));
                START_NIGHT_HOURS = Integer.parseInt(props.getProperty("n_start"));
                DURATION_NIGHT_HOURS = Integer.parseInt(props.getProperty("n_plus"));
                START_ALL_HOURS = Integer.parseInt(props.getProperty("a_start"));
                DURATION_ALL_HOURS = Integer.parseInt(props.getProperty("a_plus"));
                TIMESTAMP_CHANCE = Double.parseDouble(props.getProperty("timestamp_chance"));
            }
        } catch (IOException ie) {
            ie.printStackTrace();
        }
    }

    /**
     * Retrieves user groups for a specific user (user_id != 0) or all user groups (user_id = 0)
     * excluding USER ROLE groups
     * @return list of groups
     */
    public List<String> retrieveGroups(int user_id) {
        PreparedStatement queryStm = null;
        List<String> groups = new ArrayList<>();
        String query;
        try {
            if(user_id == 0)
                query = "SELECT distinct USER_GROUP_ID as ugid FROM USER_GROUP_MEMBERSHIP";
            else
                query = "SELECT distinct USER_GROUP_ID as ugid FROM USER_GROUP_MEMBERSHIP WHERE USER_ID = " + user_id;
            queryStm = connection.prepareStatement(query);
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) groups.add(rs.getString("ugid"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        groups.removeAll(PolicyConstants.USER_PROFILES);
        return groups;
    }

    /**
     * Retrieve user_ids belonging to a group
     * @param group_id
     * @return
     */
    public List<Integer> retrieveMembers(String group_id){
        List<Integer> user_ids = new ArrayList<>();
        PreparedStatement queryStm = null;
        try {
            queryStm = connection.prepareStatement("SELECT USER_ID as uid " +
                    "FROM USER_GROUP_MEMBERSHIP WHERE USER_GROUP_ID = ?" );
            queryStm.setString(1, String.valueOf(group_id));
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) {
                user_ids.add(rs.getInt("uid"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return user_ids;
    }

    private List<Integer> getMembers(String group_id){
        List<Integer> members = null;
        if (group_members.containsKey(group_id))
            members = group_members.get(group_id);
        else {
            members = retrieveMembers(group_id);
            group_members.put(group_id, members);
        }
        return members;
    }

    private List<String> getGroupsForUser(int user_id){
        List<String> groups = null;
        if (user_groups.containsKey(user_id))
            groups = user_groups.get(user_id);
        else {
            groups = retrieveGroups(user_id);
            user_groups.put(user_id, groups);
        }
        return groups;
    }

    private String getUserRole(int user_id){
        List<String> groups = null;
        if (user_groups.containsKey(user_id))
            groups = user_groups.get(user_id);
        else {
            groups = retrieveGroups(user_id);
            user_groups.put(user_id, groups);
        }
        String userProfile = null;
        for (String role: PolicyConstants.USER_PROFILES) {
            if(groups.contains(role)) userProfile = role;
        }
        return userProfile;
    }

    private void createPolicy(int owner, String profile, String group, int start, int duration, String action){
        List<Integer> members = getMembers(group);
        for (int querier: members) {
            if(getUserRole(querier).equalsIgnoreCase(profile)) {
                List<BEPolicy> bePolicies = pg.generatePolicies(owner, querier, start, duration, null, action);
                for (BEPolicy bePolicy: bePolicies) { //TODO: Batch insert multiple policies
                    polper.insertPolicy(bePolicy);
                }
            }
        }
    }


    private void createPolicy(int owner, String group, int start, int duration, String action){
        List<Integer> members = getMembers(group);
        for (int querier: members) {
            List<BEPolicy> bePolicies = pg.generatePolicies(owner, querier, start, duration, null, action);
            for (BEPolicy bePolicy: bePolicies) { //TODO: Batch insert multiple policies
                polper.insertPolicy(bePolicy);
            }
        }
    }

    private void createPolicy(int owner, String group, int start, int duration, List<String> locations, String action) {
        List<Integer> members = getMembers(group);
        for (int querier : members) {
            for (String location : locations) {
                List<BEPolicy> bePolicies = pg.generatePolicies(owner, querier, start, duration, location, action);
                for (BEPolicy bePolicy : bePolicies) { //TODO: Batch insert multiple policies
                    polper.insertPolicy(bePolicy);
                }
            }
        }
    }

    private void createPolicy(int owner, String profile, String group, int start, int duration, List<String> locations, String action){
        List<Integer> members = getMembers(group);
        for (int querier: members) {
            if(getUserRole(querier).equalsIgnoreCase(profile)) {
                for (String location : locations) {
                    List<BEPolicy> bePolicies = pg.generatePolicies(owner, querier, start, duration, location, action);
                    for (BEPolicy bePolicy : bePolicies) { //TODO: Batch insert multiple policies
                        polper.insertPolicy(bePolicy);
                    }
                }
            }
        }
    }

    private List<String> includeLocation(){
        int numLocations = locClusters.values().stream().mapToInt(List::size).sum();
        if (Math.random() > (float) 1/numLocations){
            return locClusters.get(r.nextInt(10));
        }
        else return null;
    }

    /**
     * Creating default and active policies for a user
     */
    public void generatePolicies(){
        List<Integer> allUsers = pg.getAllUsers();
        for (int user_id: allUsers) {
            String userProfile = getUserRole(user_id);
            List<String> groupsForUser = getGroupsForUser(user_id);
            for (String userGroup: groupsForUser) {
                //Create default policy for user groups
                createPolicy(user_id, userGroup, START_WORKING_HOURS, DURATION_WORKING_HOURS, PolicyConstants.ACTION_ALLOW);
                //Create default policy for user profiles within user groups
                createPolicy(user_id, userProfile, userGroup, START_ALL_HOURS, DURATION_ALL_HOURS, PolicyConstants.ACTION_ALLOW);

            }
            //Create default policy for faculty to staff
            if(userProfile.equalsIgnoreCase(UserProfile.FACULTY.getValue())){
                createPolicy(user_id, UserProfile.STAFF.getValue(), START_WORKING_HOURS, DURATION_WORKING_HOURS, PolicyConstants.ACTION_ALLOW);
            }
            //Create default policy for students to faculty
            if(userProfile.equalsIgnoreCase(UserProfile.GRADUATE.getValue())
                    || userProfile.equalsIgnoreCase(UserProfile.UNDERGRAD.getValue())){
                createPolicy(user_id, UserProfile.FACULTY.getValue(), START_WORKING_HOURS, DURATION_WORKING_HOURS, PolicyConstants.ACTION_ALLOW);
            }
            if(userProfile.equalsIgnoreCase(UserProfile.VISITOR.getValue())) {
                //Create default policy for visitor during night time to all
                createPolicy(user_id, UserProfile.FACULTY.getValue(), START_NIGHT_HOURS, DURATION_NIGHT_HOURS, PolicyConstants.ACTION_ALLOW);
                createPolicy(user_id, UserProfile.GRADUATE.getValue(), START_NIGHT_HOURS, DURATION_NIGHT_HOURS, PolicyConstants.ACTION_ALLOW);
                createPolicy(user_id, UserProfile.UNDERGRAD.getValue(), START_NIGHT_HOURS, DURATION_NIGHT_HOURS, PolicyConstants.ACTION_ALLOW);
                createPolicy(user_id, UserProfile.STAFF.getValue(), START_NIGHT_HOURS, DURATION_NIGHT_HOURS, PolicyConstants.ACTION_ALLOW);
                //Create default policy for visitor to staff to a list of locations during daytime
                createPolicy(user_id, UserProfile.STAFF.getValue(), START_WORKING_HOURS, DURATION_WORKING_HOURS, locClusters.get(0), PolicyConstants.ACTION_ALLOW);
            }
            //Active user policies
            if (userProfile.equalsIgnoreCase(UserProfile.VISITOR.getValue())){
                if(Math.random() < PERCENTAGE_DEFAULT) continue;
                for(int i = 0; i < ACTIVE_CHOICES; i++){
                    String querierProfile = null;
                    if(Math.random() >  (float)1/PolicyConstants.USER_PROFILES.size())
                        querierProfile = PolicyConstants.USER_PROFILES.get(new Random().nextInt(PolicyConstants.USER_PROFILES.size()));
                    int start = 0, duration = 0;
                    if(Math.random() < TIMESTAMP_CHANCE) {
                        start = r.nextInt(8) + 8;
                        duration = r.nextInt(3) + 1;
                    }
                    String group = groupsForUser.get(new Random().nextInt(groupsForUser.size()));
                    List<String> locations = includeLocation();
                    if(locations != null) {
                        if(querierProfile != null)
                            createPolicy(user_id, querierProfile, group, start, duration, locations, PolicyConstants.ACTION_ALLOW);
                        else
                            createPolicy(user_id, group, start, duration, locations, PolicyConstants.ACTION_ALLOW);
                    }
                    else {
                        if(querierProfile != null)
                            createPolicy(user_id, querierProfile, group, start, duration, PolicyConstants.ACTION_ALLOW);
                        else
                            createPolicy(user_id, group, start, duration, PolicyConstants.ACTION_ALLOW);
                    }
                }
            }
        }
    }


    public static void main(String [] args) {
        PolicyGroupGen pgrg = new PolicyGroupGen();
        pgrg.generatePolicies();


    }
}


