package edu.uci.ics.tippers.generation.policy;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.manager.PolicyPersistor;
import edu.uci.ics.tippers.model.data.UserProfile;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.TimeStampPredicate;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;

/**
 * Generation of semi-realistic policies based on group memberships of a user
 */
public class PolicyGroupGen {

    private static final int NUMBER_OF_CLUSTERS = 10;
    private Connection connection;
    Random r;
    PolicyPersistor polper;
    PolicyGen pg;

    private HashMap<Integer, List<String>> user_groups;
    private HashMap<Integer, String> user_profiles;
    private HashMap<String, List<Integer>> group_members;
    private HashMap<Integer, List<String>> location_clusters; //Forming random clusters of locations

    private final int GROUP_MEMBER_ACTIVE_LIMIT = 7;

    private final int ALL_GROUPS = 0;

    private int LARGE_GROUP_SIZE_THRESHOLD;
    private double PERCENTAGE_DEFAULT;
    private double PERCENTAGE_ALLOW;
    private int ACTIVE_CHOICES;
    private double TIMESTAMP_CHANCE;
    private TimeStampPredicate workingHours;
    private TimeStampPredicate nightHours;
    private TimeStampPredicate allHours;

    private String START_WORKING_HOURS;
    private int DURATION_WORKING_HOURS;
    private String START_NIGHT_HOURS;
    private int DURATION_NIGHT_HOURS;
    private String START_ALL_HOURS;
    private int DURATION_ALL_HOURS;


    public PolicyGroupGen(){
        this.connection = MySQLConnectionManager.getInstance().getConnection();
        r = new Random();
        polper = PolicyPersistor.getInstance();
        pg = new PolicyGen();
        user_groups = new HashMap<>();
        group_members = new HashMap<>();
        user_profiles = new HashMap<>();

        location_clusters = new HashMap<>();
        List<String> all_locations = pg.getAllLocations();
        for (int i = 0; i <  10; i++) {
            List<String> locations = new ArrayList<>();
            location_clusters.put(i, locations);
        }
        for (String loc: all_locations) {
            int cluster = r.nextInt(10);
            location_clusters.get(cluster).add(loc);
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
                TIMESTAMP_CHANCE = Double.parseDouble(props.getProperty("timestamp_chance"));
                START_WORKING_HOURS = props.getProperty("w_start");
                DURATION_WORKING_HOURS = Integer.parseInt(props.getProperty("w_plus"));
                workingHours = new TimeStampPredicate(pg.start_beg, pg.end_fin, START_WORKING_HOURS, DURATION_WORKING_HOURS);
                START_NIGHT_HOURS = props.getProperty("n_start");
                DURATION_NIGHT_HOURS = Integer.parseInt(props.getProperty("n_plus"));
                nightHours = new TimeStampPredicate(pg.start_beg, pg.end_fin, START_NIGHT_HOURS, DURATION_NIGHT_HOURS);
                START_ALL_HOURS = props.getProperty("a_start");
                DURATION_ALL_HOURS = Integer.parseInt(props.getProperty("a_plus"));
                allHours = new TimeStampPredicate(pg.start_beg, pg.end_fin, START_ALL_HOURS, DURATION_ALL_HOURS);
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
        if(user_id != ALL_GROUPS) {
            List<String> userProfiles = new ArrayList<>(groups);
            userProfiles.retainAll(PolicyConstants.USER_PROFILES);
            user_profiles.put(user_id, userProfiles.get(0));
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
                    "FROM USER_GROUP_MEMBERSHIP WHERE USER_GROUP_ID = ? " );
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


    // Function select an element base on index and return
    // an element
    public List<Integer> getRandomElement(List<Integer> list,
                                          int totalItems)
    {
        Random rand = new Random();
        List<Integer> newList = new ArrayList<>();
        int i = 0;
        if(list.size() < totalItems) return list;
        int randomIndex = rand.nextInt(list.size());
        while ( i < totalItems) {
            newList.add(list.get((randomIndex + i)  % list.size()));
            i++;
        }
        return newList;
    }

    private List<Integer> getMembers(String group_id, int limit){
        List<Integer> members = null;
        if (group_members.containsKey(group_id))
            members = group_members.get(group_id);
        else {
            members = retrieveMembers(group_id);
            group_members.put(group_id, members);
        }
        if(limit != 0 ) return getRandomElement(members, limit);
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
        if (!user_groups.containsKey(user_id)) {
            user_groups.put(user_id, retrieveGroups(user_id));
        }
        return user_profiles.get(user_id);
    }

    private void createPolicy(int owner, String profile, String group, TimeStampPredicate tsPred, String action, int limit){
        List<Integer> members = getMembers(group, limit);
        List<BEPolicy> bePolicies = new ArrayList<>();
        for (int querier: members) {
            if(getUserRole(querier).equalsIgnoreCase(profile)) {
                bePolicies.addAll(pg.generatePolicies(owner, querier, tsPred,null, action));
            }
        }
        polper.insertPolicy(bePolicies);
    }


    private void createPolicy(int owner, String group, TimeStampPredicate tsPred, String action, int limit){
        List<Integer> members = getMembers(group, limit);
        List<BEPolicy> bePolicies = new ArrayList<>();
        for (int querier: members) {
            bePolicies.addAll(pg.generatePolicies(owner, querier, tsPred, null, action));
        }
        polper.insertPolicy(bePolicies);
    }

    private void createPolicy(int owner, String group, TimeStampPredicate tsPred, List<String> locations, String action, int limit) {
        List<Integer> members = getMembers(group, limit);
        List<BEPolicy> bePolicies = new ArrayList<>();
        for (int querier : members) {
            for (String location : locations) {
                bePolicies.addAll(pg.generatePolicies(owner, querier, tsPred, location, action));
            }
        }
        polper.insertPolicy(bePolicies);
    }

    private void createPolicy(int owner, String profile, String group, TimeStampPredicate tsPred, List<String> locations,
                              String action, int limit){
        List<Integer> members = getMembers(group, limit);
        List<BEPolicy> bePolicies = new ArrayList<>();
        for (int querier: members) {
            if(getUserRole(querier).equalsIgnoreCase(profile)) {
                for (String location : locations) {
                    bePolicies.addAll(pg.generatePolicies(owner, querier, tsPred, location, action));
                }
            }
        }
        polper.insertPolicy(bePolicies);
    }

    private List<String> includeLocation(){
        if (Math.random() > (float) 1/location_clusters.size()){
            return location_clusters.get(r.nextInt(10));
        }
        else return null;
    }


    private void generateDefaultPolicies(int user_id, String userProfile, List<String> groups){
        for (String userGroup: groups) {
            //Create default policy for user group
            createPolicy(user_id, userGroup, workingHours, PolicyConstants.ACTION_ALLOW, 0);
            //Create default policy for user profiles within user groups
            createPolicy(user_id, userProfile, userGroup, allHours, PolicyConstants.ACTION_ALLOW, 0);
        }
        //Create default policy for faculty to staff
        if(userProfile.equalsIgnoreCase(UserProfile.FACULTY.getValue())){
            createPolicy(user_id, UserProfile.STAFF.getValue(), workingHours, PolicyConstants.ACTION_ALLOW, 0);
        }
        //Create default policy for students to faculty
        if(userProfile.equalsIgnoreCase(UserProfile.GRADUATE.getValue())
                || userProfile.equalsIgnoreCase(UserProfile.UNDERGRAD.getValue())){
            createPolicy(user_id, UserProfile.FACULTY.getValue(), workingHours, PolicyConstants.ACTION_ALLOW, 0);
        }
        if(userProfile.equalsIgnoreCase(UserProfile.VISITOR.getValue())) {
            //Create default policy for visitor during night time to all
            createPolicy(user_id, UserProfile.FACULTY.getValue(), nightHours, PolicyConstants.ACTION_ALLOW, 0);
            createPolicy(user_id, UserProfile.GRADUATE.getValue(), nightHours, PolicyConstants.ACTION_ALLOW, 0);
            createPolicy(user_id, UserProfile.UNDERGRAD.getValue(), nightHours, PolicyConstants.ACTION_ALLOW, 0);
            createPolicy(user_id, UserProfile.STAFF.getValue(), nightHours, PolicyConstants.ACTION_ALLOW, 0);
            //Create default policy for visitor to staff to a list of locations during daytime
            createPolicy(user_id, UserProfile.STAFF.getValue(), nightHours, location_clusters.get(0), PolicyConstants.ACTION_ALLOW, 0);
        }
    }


    private void generateActivePolicies(int user_id, String userProfile, List<String> groupsForUser) {
        for (int i = 0; i < ACTIVE_CHOICES; i++) {
            String querierProfile = null;
            if (Math.random() > (float) 1 / PolicyConstants.USER_PROFILES.size())
                querierProfile = PolicyConstants.USER_PROFILES.get(new Random().nextInt(PolicyConstants.USER_PROFILES.size()));
            int offset = 0, duration = 0, week = 0;
            if (Math.random() < TIMESTAMP_CHANCE) {
                offset = r.nextInt(8) + 1;
                offset = offset * 60; //converting to minutes
                duration = r.nextInt(3) + 1;
                duration = duration * 60;
                week = r.nextInt(12);
            }
            TimeStampPredicate tsPred = new TimeStampPredicate(pg.start_beg, week, START_WORKING_HOURS, offset, duration);
            String group = groupsForUser.get(new Random().nextInt(groupsForUser.size()));
            List<String> locations = includeLocation();
            if (locations != null) {
                if (querierProfile != null)
                    createPolicy(user_id, querierProfile, group, tsPred, locations, PolicyConstants.ACTION_ALLOW, GROUP_MEMBER_ACTIVE_LIMIT);
                else
                    createPolicy(user_id, group, tsPred, locations, PolicyConstants.ACTION_ALLOW, GROUP_MEMBER_ACTIVE_LIMIT);
            } else {
                if (querierProfile != null)
                    createPolicy(user_id, querierProfile, group, tsPred, PolicyConstants.ACTION_ALLOW, GROUP_MEMBER_ACTIVE_LIMIT);
                else
                    createPolicy(user_id, group, tsPred, PolicyConstants.ACTION_ALLOW, GROUP_MEMBER_ACTIVE_LIMIT);
            }
        }
    }


    /**
     * Creating default and active policies for a user
     */
    public void generatePolicies(){
        List<Integer> allUsers = pg.getAllUsers();
        System.out.println("Total users: " + allUsers);
        int default_count = 0, active_count = 0;
        for (int user_id: allUsers) {
            String userProfile = getUserRole(user_id);
            List<String> groupsForUser = getGroupsForUser(user_id);
            if(Math.random() < PERCENTAGE_DEFAULT) {//Default users
                generateDefaultPolicies(user_id, userProfile, groupsForUser);
                System.out.println("Default user: " + user_id);
                default_count += 1;
            }
            else { //Active users
                if (!userProfile.equalsIgnoreCase(UserProfile.VISITOR.getValue())) {
                    System.out.println("Active user: " + user_id);
                    generateActivePolicies(user_id, userProfile, groupsForUser);
                    active_count += 1;
                }
            }
        }
        System.out.println("Default count: " + default_count + " Active count: " + active_count);
    }

    public static void main(String [] args) {
        PolicyGroupGen pgrg = new PolicyGroupGen();
        pgrg.generatePolicies();
    }
}


