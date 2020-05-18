package edu.uci.ics.tippers.generation.policy.tpch;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.TimeStampPredicate;
import edu.uci.ics.tippers.model.tpch.OrderPriority;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class OrderPolicyGen {

    TPolicyGen tpg;
    double PERCENTAGE_DEFAULT;
    private int ACTIVE_CHOICES;
    private Connection connection;


    private HashMap<Integer, List<String>> order_clerks;
    private HashMap<Integer, String> order_priorities;
    private HashMap<String, List<Integer>> clerks_customers;


    public OrderPolicyGen(){
        tpg = new TPolicyGen();
        connection = MySQLConnectionManager.getInstance().getConnection();
        PERCENTAGE_DEFAULT = 0.6;
        ACTIVE_CHOICES = 40;
    }

    private List<String> getOrderClerk(int cust_key) {
        PreparedStatement queryStm = null;
        List<String> clerks = new ArrayList<>();
        String query;
        try {
            if(cust_key == 0)
                query = "SELECT distinct O_CLERK as id FROM ORDERS";
            else
                query = "SELECT distinct O_CLERK as id FROM ORDERS WHERE O_CUSTKEY = " + cust_key;
            queryStm = connection.prepareStatement(query);
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) clerks.add(rs.getString("id"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return clerks;
    }

    private List<Integer> retrieveClerkCustomer(String clerk){
        List<Integer> cust_keys = new ArrayList<>();
        PreparedStatement queryStm = null;
        try {
            queryStm = connection.prepareStatement("SELECT O_CUSTKEY as ckey " +
                    "FROM ORDERS WHERE O_CLERK  = ? " );
            queryStm.setString(1, clerk);
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) {
                cust_keys.add(rs.getInt("uid"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return cust_keys;
    }


    private List<Integer> getClerkCustomers(String group_id, int limit){
        List<Integer> c_customers = null;
        if (clerks_customers.containsKey(group_id))
            c_customers = clerks_customers.get(group_id);
        else {
            members = retrieveMembers(group_id);
            group_members.put(group_id, members);
        }
        if(limit != 0 ) return getRandomElements(members, limit);
        return members;
    }

    private String getOrderPriority(int cust_key) {
    }

    private void generateActivePolicies(int cust_key, String orderClerk) {
        List<BEPolicy> activePolicies = new ArrayList<>();
        for (int i = 0; i < ACTIVE_CHOICES; i++) {
            String querierProfile = null;
            if (Math.random() > (float) 1 / PolicyConstants.USER_PROFILES.size())
                querierProfile = PolicyConstants.USER_PROFILES.get(new Random().nextInt(PolicyConstants.USER_PROFILES.size()));
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

    private void generateDefaultPolicies(List<Integer> allCustomers) {
        List<BEPolicy> defaultPolicies = new ArrayList<>();
        for (int querier: allCustomers) {
            String querierGroup = getOrderClerk(querier);
            String querierProfile = getOrderPriority(querier);
            if (querierGroup != null) {
                //Create default policy for user group
                defaultPolicies.add(tpg.generatePolicies(querier, 0, querierGroups.get(0), null, workingHours,
                        null, PolicyConstants.ACTION_ALLOW));
                //Create default policy for user profiles within user groups
                defaultPolicies.add(tpg.generatePolicies(querier, 0, querierGroups.get(0), querierProfile, null,
                        null, PolicyConstants.ACTION_ALLOW));
            }
    }

    /**
     * Creating default and active policies for a customer
     */
    public void generatePolicies(){
        List<Integer> allCustomers = tpg.getAllCustomerKeys();
        generateDefaultPolicies(allCustomers);
        int default_count = 0, active_count = 0;
        for (int cust_key: allCustomers) {
            String orderPriority = getOrderPriority(cust_key);
            String orderClerk = getOrderClerk(cust_key);
            if(Math.random() < PERCENTAGE_DEFAULT) //Default users
                default_count += 1;
            else { //Active users
                if (!orderPriority.equalsIgnoreCase(OrderPriority.LOW.getPriority())) {
                    System.out.println("Active customer: " + cust_key);
                    generateActivePolicies(cust_key, orderClerk);
                    active_count += 1;
                }
            }
        }
        System.out.println("Default count: " + default_count + " Active count: " + active_count);
    }

    public static void main(String [] args) {
        OrderPolicyGen opg = new OrderPolicyGen();
        opg.generatePolicies();
    }


}
