package edu.uci.ics.tippers.generation.policy.tpch;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.manager.PolicyPersistor;
import edu.uci.ics.tippers.model.policy.BEPolicy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class OrderPolicyGen {

    TPolicyGen tpg;
    PolicyPersistor polper;
    private Connection connection;

    private HashMap<Integer, List<String>> customer_clerks;
    private HashMap<Integer, String> customer_profile;
    private HashMap<String, List<Integer>> clerks_customers;

    private static final double PERCENTAGE_DEFAULT = 0.6;
    private static final int ACTIVE_CHOICES = 10;
    private static final double TOTAL_PRICE_STD = 88621.40;
    private static final double TOTAL_PRICE_AVG = 151219.53;

    private double MAX_TOTAL_PRICE;
    private double MIN_TOTAL_PRICE;
    private LocalDate MAX_DATE;
    private LocalDate MIN_DATE;


    public OrderPolicyGen(){
        tpg = new TPolicyGen();
        polper = PolicyPersistor.getInstance();
        connection = MySQLConnectionManager.getInstance().getConnection();
        customer_clerks = new HashMap<>();
        customer_profile = new HashMap<>();
        clerks_customers = new HashMap<>();

        MAX_TOTAL_PRICE = tpg.getTotalPrice("MAX");
        MIN_TOTAL_PRICE = tpg.getTotalPrice("MIN");
        MAX_DATE = tpg.getOrderDate("MAX").toLocalDateTime().toLocalDate();
        MIN_DATE = tpg.getOrderDate("MIN").toLocalDateTime().toLocalDate();
    }

    private List<String> retrieveClerks(int cust_key){
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

    /**
     * Get groups of a customer
     * @return
     */
    private List<String> getClerks(int cust_key){
        List<String> clerks = null;
        if (customer_clerks.containsKey(cust_key))
            clerks = customer_clerks.get(cust_key);
        else {
            clerks = retrieveClerks(cust_key);
            customer_clerks.put(cust_key, clerks);
        }
        return clerks;
    }


    private List<Integer> retrieveClerkCustomers(String clerk){
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

    /**
     * Get customers in a group/clerk
     * @param clerk_id
     * @param limit
     * @return
     */
    private List<Integer> getCustomers(String clerk_id, int limit){
        List<Integer> c_customers = null;
        if (clerks_customers.containsKey(clerk_id))
            c_customers = clerks_customers.get(clerk_id);
        else {
            c_customers = retrieveClerkCustomers(clerk_id);
            clerks_customers.put(clerk_id, c_customers);
        }
        if(limit != 0 ) return getRandomElements(c_customers, limit);
        return c_customers;
    }

    private String retrieveProfiles(int cust_key){
        PreparedStatement queryStm = null;
        String profile = null;
        String query;
        try {
            if(cust_key == 0)
                query = "SELECT distinct O_PROFILE as id FROM ORDERS";
            else
                query = "SELECT distinct O_PROFILE as id FROM ORDERS WHERE O_CUSTKEY = " + cust_key;
            queryStm = connection.prepareStatement(query);
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) profile = rs.getString("id");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return profile;
    }

    /**
     * Get profiles of a customer
     * @param cust_key
     * @return
     */
    private String getProfiles(int cust_key) {
        String profile = null;
        if (customer_profile.containsKey(cust_key))
            profile = customer_profile.get(cust_key);
        else {
            profile = retrieveProfiles(cust_key);
            customer_profile.put(cust_key, profile);
        }
        return profile;
    }

    private List<String> randomClerks(List<String> clerks, int new_size) {
        List s_clerks = new ArrayList<>(clerks);
        Collections.shuffle(s_clerks);
        return s_clerks.subList(0, new_size);
    }

    private void generateDefaultPolicies(List<Integer> allCustomers) {
        List<BEPolicy> defaultPolicies = new ArrayList<>();
        for (int k = 0; k < 2; k++) { //DEBUGGING
            int querier = allCustomers.get(k);
            List<String> querierGroups = getClerks(querier);
            String querierProfile = getProfiles(querier);
            if (querierGroups != null || !querierGroups.isEmpty()) {
                int newSize = Math.min(querierGroups.size(), 3);
                List<String> s_clerks = randomClerks(querierGroups, newSize);
                PricePredicate totalPrice = new PricePredicate(TOTAL_PRICE_AVG - TOTAL_PRICE_STD/2, TOTAL_PRICE_AVG + 2*TOTAL_PRICE_STD);
                LocalDate startDate = this.MIN_DATE.plus(2, ChronoUnit.YEARS);
                LocalDate endDate = this.MAX_DATE.minus(2, ChronoUnit.YEARS);
                DatePredicate datePred = new DatePredicate(startDate, endDate);
                for (int i = 0; i < s_clerks.size(); i++) {
                    //Create default policy for user group
                    defaultPolicies.add(tpg.generatePolicies(querier, 0, s_clerks.get(i), null, totalPrice,
                            datePred, PolicyConstants.ACTION_ALLOW));
                    //Create default policy for user profiles within user groups
                    defaultPolicies.add(tpg.generatePolicies(querier, 0,  s_clerks.get(i), querierProfile, totalPrice,
                            datePred, PolicyConstants.ACTION_ALLOW));
                }
            }
        }
        polper.insertPolicy(defaultPolicies);
    }


//        private void generateActivePolicies(int cust_key, String orderClerk) {
//        List<BEPolicy> activePolicies = new ArrayList<>();
//        for (int i = 0; i < ACTIVE_CHOICES; i++) {
//            String querierProfile = null;
//            if (Math.random() > (float) 1 / PolicyConstants.USER_PROFILES.size())
//                querierProfile = PolicyConstants.USER_PROFILES.get(new Random().nextInt(PolicyConstants.USER_PROFILES.size()));
//            String querierGroup = owner_groups.get(0); //number of groups per user is 1
//            List<Integer> queriers = getListOfUsers(querierGroup, querierProfile, GROUP_MEMBER_ACTIVE_LIMIT);
//            int offset = 0, duration = 0, week = 0;
//            if (Math.random() < TIMESTAMP_CHANCE) {
//                offset = r.nextInt(8) + 1;
//                offset = offset * 60; //converting to minutes
//                duration = r.nextInt(3) + 1;
//                duration = duration * 60;
//                week = r.nextInt(12);
//            }
//            TimeStampPredicate tsPred = new TimeStampPredicate(pg.getDate("MIN"), week, START_WORKING_HOURS, offset, duration);
//            List<String> locations = includeLocation();
//            for (int querier : queriers) {
//                if(querier == owner_id) continue;
//                if (locations != null)
//                    for (String loc : locations)
//                        activePolicies.add(pg.generatePolicies(querier, owner_id, null, null, tsPred,
//                                loc, PolicyConstants.ACTION_ALLOW));
//                else
//                    activePolicies.add(pg.generatePolicies(querier, owner_id, null, null, tsPred,
//                            null, PolicyConstants.ACTION_ALLOW));
//            }
//        }
//        polper.insertPolicy(activePolicies);
//    }


    /**
     * Creating default and active policies for a customer
     */
    public void generatePolicies(){
        List<Integer> allCustomers = tpg.getAllCustomerKeys();
        generateDefaultPolicies(allCustomers);
        int default_count = 0, active_count = 0;
//        for (int cust_key: allCustomers) {
//            String orderPriority = getOrderPriority(cust_key);
//            String orderClerk = getClerk(cust_key);
//            if(Math.random() < PERCENTAGE_DEFAULT) //Default users
//                default_count += 1;
//            else { //Active users
//                if (!orderPriority.equalsIgnoreCase(OrderPriority.LOW.getPriority())) {
//                    System.out.println("Active customer: " + cust_key);
//                    generateActivePolicies(cust_key, orderClerk);
//                    active_count += 1;
//                }
//            }
//        }
        System.out.println("Default count: " + default_count + " Active count: " + active_count);
    }

    public static void main(String [] args) {
        OrderPolicyGen opg = new OrderPolicyGen();
        opg.generatePolicies();
    }


}
