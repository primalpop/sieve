package edu.uci.ics.tippers.generation.policy.WiFiDataSet;

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
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Generation of semi-realistic policies based on group memberships of a user
 */
public class PolicyGroupGen {

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
    private TimeStampPredicate nightDuskHours;
    private TimeStampPredicate nightDawnHours;

    private String START_WORKING_HOURS;
    private int DURATION_WORKING_HOURS;
    private String START_NIGHT_DUSK_HOURS;
    private String START_NIGHT_DAWN_HOURS;
    private int DURATION_NIGHT_DUSK_HOURS;
    private int DURATION_NIGHT_DAWN_HOURS;

    private List<String> USER_PROFILES;


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

        USER_PROFILES = Stream.of(UserProfile.values()).map(UserProfile::getValue).collect(Collectors.toList());

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
                workingHours = new TimeStampPredicate(pg.getDate("MIN"), pg.getDate("MAX"), START_WORKING_HOURS, DURATION_WORKING_HOURS);
                START_NIGHT_DUSK_HOURS = props.getProperty("n_dusk_start");
                DURATION_NIGHT_DUSK_HOURS = Integer.parseInt(props.getProperty("n_dusk_plus"));
                nightDuskHours = new TimeStampPredicate(pg.getDate("MIN"), pg.getDate("MAX"), START_NIGHT_DUSK_HOURS, DURATION_NIGHT_DUSK_HOURS);
                START_NIGHT_DAWN_HOURS = props.getProperty("n_dawn_start");
                DURATION_NIGHT_DAWN_HOURS = Integer.parseInt(props.getProperty("n_dawn_plus"));
                nightDawnHours = new TimeStampPredicate(pg.getDate("MIN"), pg.getDate("MAX"), START_NIGHT_DAWN_HOURS, DURATION_NIGHT_DAWN_HOURS);
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
            userProfiles.retainAll(USER_PROFILES);
            user_profiles.put(user_id, userProfiles.get(0));
        }
        groups.removeAll(USER_PROFILES);
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


    // Function select an element based on index and return an element
    public List<Integer> getRandomElements(List<Integer> list, int limit) {
        Random rand = new Random();
        List<Integer> newList = new ArrayList<>();
        int i = 0;
        if(list.size() < limit) return list;
        int randomIndex = rand.nextInt(list.size());
        while ( i < limit) {
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
        if(limit != 0 ) return getRandomElements(members, limit);
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

    private List<Integer> getListOfUsers(String group, String profile, int limit){
        List<Integer> users = new ArrayList<>();
        List<Integer> members = getMembers(group, limit);
        if (profile == null) return members;
        for (int u: members) {
            if (getUserRole(u).equalsIgnoreCase(profile))
                users.add(u);
        }
        return users;
    }

    private List<String> includeLocation(){
        if (Math.random() > (float) 1/location_clusters.size()){
            return location_clusters.get(r.nextInt(10));
        }
        else return null;
    }

    /**
     * Default policies generated per querier based on their profile and group memberships
     * @param queriers
     */
    private void generateDefaultPolicies(List<Integer> queriers){
        List<BEPolicy> defaultPolicies = new ArrayList<>();
        for (int querier: queriers) {
            List<String> querierGroups = getGroupsForUser(querier);
            String querierProfile = getUserRole(querier);
            if (!querierGroups.isEmpty()) {
                //Create default policy for user group
                defaultPolicies.add(pg.generatePolicies(querier, 0, querierGroups.get(0), null, workingHours,
                        null, PolicyConstants.ACTION_ALLOW));
                //Create default policy for user profiles within user groups
                defaultPolicies.add(pg.generatePolicies(querier, 0, querierGroups.get(0), querierProfile, null,
                        null, PolicyConstants.ACTION_ALLOW));
            }
            //Create default policy for staff to monitor faculty and visitors
//            if(querierProfile.equalsIgnoreCase(UserProfile.STAFF.getValue())){
//                defaultPolicies.add(pg.generatePolicies(querier, 0, null, UserProfile.FACULTY.getValue(),
//                        workingHours, null, PolicyConstants.ACTION_ALLOW));
//                // in specific locations during working hours
//                for (String forbiddenLoc: location_clusters.get(0)) {
//                    defaultPolicies.add(pg.generatePolicies(querier, 0, null, UserProfile.VISITOR.getValue(),
//                            workingHours, forbiddenLoc, PolicyConstants.ACTION_ALLOW));
//                }
//            }
//            //Create default policy for faculty to see students during working hours
//            if(querierProfile.equalsIgnoreCase(UserProfile.FACULTY.getValue())){
//                defaultPolicies.add(pg.generatePolicies(querier, 0, null, UserProfile.GRADUATE.getValue(),
//                        workingHours, null, PolicyConstants.ACTION_ALLOW));
//                defaultPolicies.add(pg.generatePolicies(querier, 0, null, UserProfile.UNDERGRAD.getValue(),
//                        workingHours, null, PolicyConstants.ACTION_ALLOW));
//            }
//            //Create default policy for non-visitors to monitor visitors during night time
//            if(!querierProfile.equalsIgnoreCase(UserProfile.VISITOR.getValue())) {
//                defaultPolicies.add(pg.generatePolicies(querier, 0, null, UserProfile.VISITOR.getValue(),
//                        nightDawnHours, null,  PolicyConstants.ACTION_ALLOW));
//                defaultPolicies.add(pg.generatePolicies(querier, 0, null, UserProfile.VISITOR.getValue(),
//                        nightDuskHours, null,  PolicyConstants.ACTION_ALLOW));
//            }
        }
        polper.insertPolicy(defaultPolicies);
    }

    /**
     * Active policies generated per owner
     * @param owner_id
     * @param owner_groups
     */
    private void generateActivePolicies(int owner_id, List<String> owner_groups) {
        List<BEPolicy> activePolicies = new ArrayList<>();
        for (int i = 0; i < ACTIVE_CHOICES; i++) {
            String querierProfile = null;
            if (Math.random() > (float) 1 / USER_PROFILES.size())
                querierProfile = USER_PROFILES.get(new Random().nextInt(USER_PROFILES.size()));
            String querierGroup = owner_groups.get(0); //number of groups per user is 1
            List<Integer> queriers = getListOfUsers(querierGroup, querierProfile, GROUP_MEMBER_ACTIVE_LIMIT);
            int offset = 0, duration = 0, week = 0;
            if (Math.random() < TIMESTAMP_CHANCE) {
                offset = r.nextInt(8) + 1;
                offset = offset * 60; //converting to minutes
                duration = r.nextInt(3) + 1;
                duration = duration * 60;
                week = r.nextInt(12);
            }
            TimeStampPredicate tsPred = new TimeStampPredicate(pg.getDate("MIN"), week, START_WORKING_HOURS, offset, duration);
            List<String> locations = includeLocation();
            for (int querier : queriers) {
                if(querier == owner_id) continue;
                if (locations != null)
                    for (String loc : locations)
                        activePolicies.add(pg.generatePolicies(querier, owner_id, null, null, tsPred,
                                loc, PolicyConstants.ACTION_ALLOW));
                else
                    activePolicies.add(pg.generatePolicies(querier, owner_id, null, null, tsPred,
                            null, PolicyConstants.ACTION_ALLOW));
            }
        }
        polper.insertPolicy(activePolicies);
    }


    /**
     * Creating default and active policies for a user
     */
    public void generatePolicies(){
        List<Integer> allUsers = pg.getAllUsers(false);
        generateDefaultPolicies(allUsers);
        int default_count = 0, active_count = 0;
        for (int user_id: allUsers) {
            String userProfile = getUserRole(user_id);
            List<String> groupsForUser = getGroupsForUser(user_id);
            if(Math.random() < PERCENTAGE_DEFAULT) //Default users
                default_count += 1;
            else { //Active users
                if (!userProfile.equalsIgnoreCase(UserProfile.VISITOR.getValue())) {
                    System.out.println("Active user: " + user_id);
                    generateActivePolicies(user_id, groupsForUser);
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


