package edu.uci.ics.tippers.generation.policy;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.manager.PolicyPersistor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class PolicyGroupGen {

    private Connection connection;
    Random r;
    PolicyPersistor polper;
    PolicyGen pg;

    private HashMap<Integer, List<String>> user_groups;
    private HashMap<String, List<Integer>> group_members;

    public final int MIN_GROUP_MEMBERSHIP = 3;
    private final double NON_GROUP_CHANCE = 3/4.0;
    private final int ALL_GROUPS = 0;
    private final int ROLE_POLICIES_COUNT= 10;

    private final int LARGE_GROUP_SIZE_THRESHOLD = 50;
    private final int DEFAULT_POLICY_NUM = 5;

    public PolicyGroupGen(){
        this.connection = MySQLConnectionManager.getInstance().getConnection();
        r = new Random();
        polper = PolicyPersistor.getInstance();
        pg = new PolicyGen();
        user_groups = new HashMap<>();
        group_members = new HashMap<>();
    }

    /**
     * Retrieves users with group membership > requisite
     * @return list of users
     */
    public List<Integer> retrieveNotLoners(int requisite) {
        PreparedStatement queryStm = null;
        List<Integer> user_ids = new ArrayList<>();
        try {
            queryStm = connection.prepareStatement("SELECT USER_ID as uid, count(*) as c " +
                    "FROM USER_GROUP_MEMBERSHIP group by USER_ID having c > " + requisite);
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) user_ids.add(rs.getInt("uid"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return user_ids;
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
        groups.removeAll(PolicyConstants.USER_ROLES);
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

    private void createPolicy(int querier, List<Integer> members, int howMany){
        int i = 0;
        List<Integer> selected = new ArrayList<>();
        while (i < howMany) {
            int owner = members.get(new Random().nextInt(members.size()));
            if (selected.contains(owner)) continue;
            selected.add(owner);
            polper.insertPolicy(pg.createPolicy(querier, owner, getUserRole(owner)));
            i++;
        }
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

    private int howMany(int group_size, boolean member){
        int x = (int) (DEFAULT_POLICY_NUM + (Math.random() * (DEFAULT_POLICY_NUM - 5)));
        int multiplier = member? 1: 2;
        if(group_size < LARGE_GROUP_SIZE_THRESHOLD) {
            x = (int) (r.nextGaussian() * group_size/(20*multiplier) + group_size/(10*multiplier));
            if (x <= 1 || x > group_size) x = group_size/(10*multiplier);
        }
        return x;
    }

    private String getUserRole(int user_id){
        List<String> groups = null;
        if (user_groups.containsKey(user_id))
            groups = user_groups.get(user_id);
        else {
            groups = retrieveGroups(user_id);
            user_groups.put(user_id, groups);
        }
        for (String role: PolicyConstants.USER_ROLES) {
            if(groups.contains(role)) return role;
        }
        return null;
    }

    /**
     * 1. Get all non-loners
     * 2. For each user in non-loners: get their groups
     * 3. For each group, get members; estimate size of the group(gs) and compute x
     * (from a distribution with mean gs/5 and variance gs/10)
     * 4. Select x group members and create policy for them
     * 5. Non-groups = totalGroups - user.groups
     * 6. Compute y = random int between [1, #Non-groups/2]
     * 7. Select y Non-groups and repeat steps 3, 4
     * 8. Roles = user.roles
     * 9. For each role, select 10 members and create policy for them
     */
    public void generatePolicies(){
        List<Integer> nls = retrieveNotLoners(MIN_GROUP_MEMBERSHIP);
        List<String> all_groups = retrieveGroups(ALL_GROUPS);
        for (int querier: nls) {
            System.out.println("Querier " + querier + " group policies");
            List<String> querierGroups = retrieveGroups(querier);
            for (String qg: querierGroups) {
                List<Integer> members = getMembers(qg);
                int x = howMany(members.size(), true);
                createPolicy(querier, members, x);
            }
            System.out.println("Querier " + querier + " non-group policies");
            List<String> nonGroups = new ArrayList<>(all_groups);
            nonGroups.removeAll(querierGroups);
            for(String ng: nonGroups) {
                boolean selected = Math.random() >= (NON_GROUP_CHANCE);
                if (selected) {
                    List<Integer> members = getMembers(ng);
                    int x = howMany(members.size(), false);
                    createPolicy(querier, members, x);
                }
            }
            System.out.println("Querier " + querier + " role policies");
            for (String role: PolicyConstants.USER_ROLES) {
                List<Integer> members = retrieveMembers(role);
                if(members.isEmpty()) continue;
                int x = (int) (5 + (Math.random() * (ROLE_POLICIES_COUNT- 5)));
                createPolicy(querier, members, x);
            }
        }
    }

    //TODO: add accept and deny policies
    //TODO: for each querier, add higher percentage of policies to their own role

    public static void main(String [] args) {
        PolicyGroupGen pgrg = new PolicyGroupGen();
        pgrg.generatePolicies();
    }
}


