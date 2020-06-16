package edu.uci.ics.tippers.generation.policy.tpch;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.dbms.mysql.MySQLConnectionManager;
import edu.uci.ics.tippers.persistor.PolicyPersistor;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.tpch.OrderProfile;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OrderPolicyGen {

    TPolicyGen tpg;
    PolicyPersistor polper;
    private Connection connection;
    Random r;

    private HashMap<Integer, List<String>> customer_clerks;
    private HashMap<Integer, String> customer_profile;
    private HashMap<String, List<Integer>> clerks_customers;
    private static final List<String> ORDER_PROFILES = Stream.of(OrderProfile.values()).map(OrderProfile::getPriority).collect(Collectors.toList());


    private final int GROUP_MEMBER_ACTIVE_LIMIT = 10;
    private final double TIMESTAMP_CHANCE = 0.3;
    private static final double PERCENTAGE_DEFAULT = 0.6;
    private static final int ACTIVE_CHOICES = 10;
    private static final int ACTIVE_GROUP_CHOICES = 5;

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
        r = new Random();
        customer_clerks = new HashMap<>();
        customer_profile = new HashMap<>();
        clerks_customers = new HashMap<>();

        MAX_TOTAL_PRICE = tpg.getTotalPrice("MAX");
        MIN_TOTAL_PRICE = tpg.getTotalPrice("MIN");
        MAX_DATE = tpg.getOrderDate("MAX").toLocalDateTime().toLocalDate();
        MIN_DATE = tpg.getOrderDate("MIN").toLocalDateTime().toLocalDate();
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

    public List<String> retrieveClerks(int cust_key){
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


    public List<Integer> retrieveClerkCustomers(String clerk){
        List<Integer> cust_keys = new ArrayList<>();
        PreparedStatement queryStm = null;
        try {
            queryStm = connection.prepareStatement("SELECT O_CUSTKEY as ckey " +
                    "FROM ORDERS WHERE O_CLERK  = ? " );
            queryStm.setString(1, clerk);
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) {
                cust_keys.add(rs.getInt("ckey"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return cust_keys;
    }

    /**
     * Get profiles of a customer
     * @param cust_key
     * @return
     */
    private String getProfile(int cust_key) {
        String profile = null;
        if (customer_profile.containsKey(cust_key))
            profile = customer_profile.get(cust_key);
        else {
            profile = retrieveProfiles(cust_key);
            customer_profile.put(cust_key, profile);
        }
        return profile;
    }

    private List<Integer> getListOfCustomers(String clerk, String profile, int limit){
        List<Integer> customers = new ArrayList<>();
        List<Integer> members = getCustomers(clerk, limit);
        if (profile == null) return members;
        for (int u: members) {
            if (getProfile(u).equalsIgnoreCase(profile))
                customers.add(u);
        }
        return customers;
    }


    private List<String> randomClerks(List<String> clerks, int new_size) {
        List s_clerks = new ArrayList<>(clerks);
        Collections.shuffle(s_clerks);
        return s_clerks.subList(0, new_size);
    }

    private void generateDefaultPolicies(List<Integer> allCustomers) {
        List<BEPolicy> defaultPolicies = new ArrayList<>();
        int count = 0;
        for (int k = 0; k < allCustomers.size(); k++) {
            int querier = allCustomers.get(k);
            List<String> querierGroups = getClerks(querier);
            String querierProfile = getProfile(querier);
            if (querierGroups != null || !querierGroups.isEmpty()) {
                int newSize = Math.min(3 + r.nextInt(querierGroups.size()), querierGroups.size());
                List<String> s_clerks = randomClerks(querierGroups, newSize);
//                PricePredicate totalPrice = new PricePredicate(TOTAL_PRICE_AVG - TOTAL_PRICE_STD/2, TOTAL_PRICE_AVG + 2*TOTAL_PRICE_STD);
                LocalDate startDate = this.MIN_DATE.plus(r.nextInt(3), ChronoUnit.YEARS);
                LocalDate endDate = this.MAX_DATE.minus(r.nextInt(3), ChronoUnit.YEARS);
                DatePredicate datePred = new DatePredicate(startDate, endDate);
                for (int i = 0; i < s_clerks.size(); i++) {
                    //Create default policy for user group
                    defaultPolicies.add(tpg.generatePolicies(querier, 0, s_clerks.get(i), null, null,
                            datePred, null, PolicyConstants.ACTION_ALLOW));
                    //Create default policy for user profiles within user groups
                    defaultPolicies.add(tpg.generatePolicies(querier, 0,  s_clerks.get(i), querierProfile, null,
                            null, null, PolicyConstants.ACTION_ALLOW));
                }
            }
            if(defaultPolicies.size() % 10000 == 0) {
                polper.insertPolicy(defaultPolicies);
                count+=defaultPolicies.size();
                System.out.println("Default policies inserted: "  + count);
                defaultPolicies.clear();
            }
        }
        polper.insertPolicy(defaultPolicies);
        count+=defaultPolicies.size();
        System.out.println("Default policies completed at  "  + count + " policies");
    }


    private void generateActivePolicies(int cust_key, List<String> orderClerks) {
        List<BEPolicy> activePolicies = new ArrayList<>();
        List<String> s_clerks = randomClerks(orderClerks, Math.min(ACTIVE_GROUP_CHOICES, orderClerks.size()));
        for (int i = 0; i < ACTIVE_CHOICES; i++) {
            String querierProfile = null;
            if (Math.random() > (float) 1 / ORDER_PROFILES.size())
                querierProfile = ORDER_PROFILES.get(new Random().nextInt(ORDER_PROFILES.size()));
            for (int j = 0; j < s_clerks.size(); j++) {
                List<Integer> queriers = getListOfCustomers(s_clerks.get(j), querierProfile, GROUP_MEMBER_ACTIVE_LIMIT);
                DatePredicate datePred = null;
                if (Math.random() > TIMESTAMP_CHANCE) {
                    int offset = Math.max(2, r.nextInt(27));
                    datePred = new DatePredicate(
                            tpg.getOrderDate("MIN").toLocalDateTime().toLocalDate().plus(offset * 4 - 3, ChronoUnit.MONTHS),
                            tpg.getOrderDate("MIN").toLocalDateTime().toLocalDate().plus(offset * 4 + 3, ChronoUnit.MONTHS));
                }
                double seed = r.nextGaussian() * TOTAL_PRICE_STD + TOTAL_PRICE_AVG;
                int price_offset = Math.max(1, r.nextInt(5));
                PricePredicate totalPricePed = new PricePredicate(seed - TOTAL_PRICE_STD / price_offset, seed + TOTAL_PRICE_STD / price_offset);
                String orderPriorityPred = ORDER_PROFILES.get(r.nextInt(ORDER_PROFILES.size()));
                for (int querier : queriers) {
                    if (querier == cust_key) continue;
                    activePolicies.add(tpg.generatePolicies(querier, cust_key, null, null, totalPricePed,
                            datePred, orderPriorityPred, PolicyConstants.ACTION_ALLOW));
                }
            }
        }
        polper.insertPolicy(activePolicies);
        System.out.println("Active customer: " + cust_key + "with " + activePolicies.size() + " policies");
    }


    /**
     * Creating default and active policies for a customer
     */
    public void generatePolicies(){
        List<Integer> allCustomers = tpg.getAllCustomerKeys();
        generateDefaultPolicies(allCustomers);
        int default_count = 0, active_count = 0;
        for (int cust_key: allCustomers) {
            String profile = getProfile(cust_key);
            List<String> orderClerks = getClerks(cust_key);
            if(Math.random() > PERCENTAGE_DEFAULT) { //Active users
                if (!profile.equalsIgnoreCase(OrderProfile.LOW.getPriority())) {
                    generateActivePolicies(cust_key, orderClerks);
                    active_count += 1;
                }
            }
            default_count += 1; //
        }
        System.out.println("Default count: " + default_count + " Active count: " + active_count);
    }

    public static void main(String [] args) {
        OrderPolicyGen opg = new OrderPolicyGen();
        opg.generatePolicies();
    }


}
