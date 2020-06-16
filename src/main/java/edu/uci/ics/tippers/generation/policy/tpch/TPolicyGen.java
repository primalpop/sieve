package edu.uci.ics.tippers.generation.policy.tpch;

import edu.uci.ics.tippers.common.AttributeType;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.dbms.mysql.MySQLConnectionManager;
import edu.uci.ics.tippers.model.policy.*;

import java.sql.*;
import java.util.*;

public class TPolicyGen {


    private Connection connection;
    private Random r;
    private List<Integer> customer_keys;
    private List<String> customer_clerks;
    private List<String> customer_profiles;
    private double startTotPrice, endTotPrice;
    private Timestamp startOrderDate = null, endOrderDate = null;

    public TPolicyGen(){
        r = new Random();
        connection = MySQLConnectionManager.getInstance().getConnection();
    }

    public List<Integer> getAllCustomerKeys() {
        PreparedStatement queryStm = null;
        customer_keys = new ArrayList<>();
        try {
            queryStm = connection.prepareStatement("SELECT distinct O_CUSTKEY as id FROM ORDERS order by O_CUSTKEY");
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) customer_keys.add(rs.getInt("id"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return customer_keys;
    }

    public List<String> getAllClerks() {
        PreparedStatement queryStm = null;
        customer_clerks = new ArrayList<>();
        try {
            queryStm = connection.prepareStatement("SELECT distinct O_CLERK as id FROM ORDERS order by O_CLERK");
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) customer_clerks.add(rs.getString("id"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return customer_clerks;
    }

    public List<String> getAllProfiles() {
        PreparedStatement queryStm = null;
        customer_profiles = new ArrayList<>();
        try {
            queryStm = connection.prepareStatement("SELECT O_PROFILE as op FROM ORDERS");
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) customer_profiles.add(rs.getString("op"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return customer_profiles;
    }

    public Timestamp retrieveDate(String valType){
        PreparedStatement queryStm = null;
        Timestamp ts = null;
        try{
            queryStm = connection.prepareStatement("SELECT " + valType + "(O_ORDERDATE) AS value from ORDERS");
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) ts = rs.getTimestamp("value");
        } catch(SQLException e) {
            e.printStackTrace();
        }
        return ts;
    }

    public Timestamp getOrderDate(String valType){
        if (valType.equalsIgnoreCase("MIN")) {
            if(startOrderDate == null) startOrderDate = retrieveDate(valType);
            return startOrderDate;
        } else {
            if(endOrderDate == null) endOrderDate = retrieveDate(valType);
            return endOrderDate;
        }
    }


    public double retrieveTotalPrice(String valType){
        PreparedStatement queryStm = null;
        double total_price = 0;
        try{
            queryStm = connection.prepareStatement("SELECT " + valType + "(O_TOTALPRICE) AS value from ORDERS");
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) total_price = rs.getDouble("value");
        } catch(SQLException e) {
            e.printStackTrace();
        }
        return total_price;
    }



    public double getTotalPrice(String valType){
        if (valType.equalsIgnoreCase("MIN")) {
            if(startTotPrice == 0) startTotPrice = retrieveTotalPrice(valType);
            return startTotPrice;
        } else {
            if(endTotPrice == 0) endTotPrice = retrieveTotalPrice(valType);
            return endTotPrice;
        }
    }




    /**
     * @param querier - customer key of the user to whom policy applies
     * @param o_custkey - customer key of the tuple
     * @param o_clerk - clerk of the tuple (1000 values) mimics owner_group
     * @param o_profile - order profile of the tuple  (6 values) the maximum of order_priority values for a customer
     * @param totalPricePred  - low and high range predicates of total price
     * @param datePred - low and high range predicates of date
     * @param action - allow
     * @param
     * @return
     */
    public BEPolicy generatePolicies(int querier, int o_custkey, String o_clerk, String o_profile,
                                     PricePredicate totalPricePred, DatePredicate datePred, String orderPriority, String action) {
        String policyID = UUID.randomUUID().toString();
        List<QuerierCondition> querierConditions = new ArrayList<>(Arrays.asList(
                new QuerierCondition(policyID, "policy_type", AttributeType.STRING, Operation.EQ, "user"),
                new QuerierCondition(policyID, "querier", AttributeType.STRING, Operation.EQ, String.valueOf(querier))));
        List<ObjectCondition> objectConditions = new ArrayList<>();
        if (o_custkey != 0) {
            ObjectCondition owner = new ObjectCondition(policyID, PolicyConstants.ORDER_CUSTOMER_KEY, AttributeType.STRING,
                    String.valueOf(o_custkey), Operation.EQ);
            objectConditions.add(owner);
        }
        if (o_clerk != null){
            ObjectCondition ownerGroup = new ObjectCondition(policyID, PolicyConstants.ORDER_CLERK, AttributeType.STRING,
                    o_clerk, Operation.EQ);
            objectConditions.add(ownerGroup);
        }
        if (o_profile != null) {
            ObjectCondition ownerProfile = new ObjectCondition(policyID, PolicyConstants.ORDER_PROFILE, AttributeType.STRING,
                    o_profile, Operation.EQ);
            objectConditions.add(ownerProfile);
        }
        if (datePred != null) {
            ObjectCondition datePredicate = new ObjectCondition(policyID, PolicyConstants.ORDER_DATE, AttributeType.DATE,
                    datePred.getStartDate().toString(), Operation.GTE, datePred.getEndDate().toString(), Operation.LTE);
            objectConditions.add(datePredicate);
        }
        if (totalPricePred != null) {
            ObjectCondition totalPricePredicate = new ObjectCondition(policyID, PolicyConstants.ORDER_TOTAL_PRICE, AttributeType.DOUBLE,
                    String.valueOf(totalPricePred.getLow_range()), Operation.GTE, String.valueOf(totalPricePred.getHigh_range()), Operation.LTE);
            objectConditions.add(totalPricePredicate);
        }
        if (orderPriority != null) {
            ObjectCondition orderPriorityPredicate = new ObjectCondition(policyID, PolicyConstants.ORDER_PRIORITY, AttributeType.STRING,
                    orderPriority, Operation.EQ);
            objectConditions.add(orderPriorityPredicate);
        }
        if(objectConditions.isEmpty()){
            System.out.println("Empty Object Conditions");
        }
        return new BEPolicy(policyID, objectConditions, querierConditions, "benchmark",
                action, new Timestamp(System.currentTimeMillis()));
    }
}
