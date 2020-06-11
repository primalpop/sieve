package edu.uci.ics.tippers.generation.policy.Mall;

import edu.uci.ics.tippers.common.AttributeType;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.model.policy.*;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;

public class MPolicyGen {

    private final Connection connection;


    public MPolicyGen(){
        connection = PolicyConstants.getDBMSConnection();
    }

    public List<Integer> retrieveAllDevices() {
        PreparedStatement queryStm = null;
        List<Integer> devices = new ArrayList<>();
        try {
            queryStm = connection.prepareStatement("SELECT distinct device_id as id FROM MALL_OBSERVATION order by device_id");
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) devices.add(rs.getInt("id"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return devices;
    }

    public List<String> retrieveAllInterests() {
        PreparedStatement queryStm = null;
        List<String> user_interests = new ArrayList<>();
        try {
            queryStm = connection.prepareStatement("SELECT distinct user_interest as id FROM MALL_OBSERVATION order by id");
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) user_interests.add(rs.getString("id"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return user_interests;
    }

    public LocalDate retrieveDate(String valType){
        PreparedStatement queryStm = null;
        LocalDate ld = null;
        try{
            queryStm = connection.prepareStatement("SELECT " + valType + "(obs_date) AS value from MALL_OBSERVATION");
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) ld = rs.getDate("value").toLocalDate();
        } catch(SQLException e) {
            e.printStackTrace();
        }
        return ld;
    }

    /**
     * Get a map of user to interest
     * @return
     */
    public Map<Integer, String> retrieveUserInterest(){
        Map<Integer, String> userToInterest = new HashMap<>();
        PreparedStatement queryStm = null;
        try {
            queryStm = connection.prepareStatement("Select device_id, user_interest, count(*) from MALL_OBSERVATION " +
                    "group by device_id, user_interest");
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) {
                int device_id = rs.getInt("device_id");
                String user_interest = rs.getString("user_interest");
                userToInterest.put(device_id, user_interest);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return userToInterest;
    }

    /**
     * Get a map of user to shops where shops are ordered by the number of visits
     * @return
     */
    public Map<Integer, List<String>> retrieveUserShops(){
        Map<Integer, List<String>> userToShop = new HashMap<>();
        PreparedStatement queryStm = null;
        try {
            queryStm = connection.prepareStatement("SELECT device_id, shop_name, count(*) as c " +
                    "FROM MALL_OBSERVATION group by device_id, shop_name order by device_id, c desc");
            ResultSet rs = queryStm.executeQuery();
            int next_device;
            int device_id = 0;
            if (!rs.next()) return null;
            while (true) {
                if (device_id == 0) {
                    device_id = rs.getInt("device_id");
                    List<String> shop_names = new ArrayList<>();
                    userToShop.put(device_id, shop_names);
                }
                userToShop.get(device_id).add(rs.getString("shop_name"));
                if (!rs.next()) //last user and shop
                    break;
                next_device = rs.getInt( "device_id");
                if (device_id != next_device)
                    device_id = 0;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return userToShop;
    }

    /**
     * @param querier - shop name
     * @param device_id - id of the owner device of the tuple
     * @param shop_name - location of the tuple
     * @param tsPred - time period captured using date and time
     * @param user_interest - one of the six shop_types
     * @param action - allow or deny
     * @return
     */
    public BEPolicy generatePolicies(int querier, int device_id, String shop_name, TimeStampPredicate tsPred,
                                     String user_interest, String action) {
        String policyID = UUID.randomUUID().toString();
        List<QuerierCondition> querierConditions = new ArrayList<>(Arrays.asList(
                new QuerierCondition(policyID, "policy_type", AttributeType.STRING, Operation.EQ, "user"),
                new QuerierCondition(policyID, "querier", AttributeType.STRING, Operation.EQ, String.valueOf(querier))));
        List<ObjectCondition> objectConditions = new ArrayList<>();
        if (device_id != 0) {
            ObjectCondition owner = new ObjectCondition(policyID, PolicyConstants.M_DEVICE, AttributeType.STRING,
                    String.valueOf(device_id), Operation.EQ);
            objectConditions.add(owner);
        }
        if(shop_name != null) {
            ObjectCondition shop = new ObjectCondition(policyID, PolicyConstants.M_SHOP_NAME, AttributeType.STRING,
                    shop_name, Operation.EQ);
            objectConditions.add(shop);
        }
        if (tsPred != null) {
            ObjectCondition datePred = new ObjectCondition(policyID, PolicyConstants.M_DATE, AttributeType.DATE,
                    tsPred.getStartDate().toString(), Operation.GTE, tsPred.getEndDate().toString(), Operation.LTE);
            objectConditions.add(datePred);
            ObjectCondition timePred = new ObjectCondition(policyID, PolicyConstants.M_TIME, AttributeType.TIME,
                    tsPred.parseStartTime(), Operation.GTE, tsPred.parseEndTime(), Operation.LTE);
            objectConditions.add(timePred);
        }
        if(user_interest != null){
            ObjectCondition interest = new ObjectCondition(policyID, PolicyConstants.M_INTEREST, AttributeType.STRING,
                    user_interest, Operation.EQ);
            objectConditions.add(interest);
        }
        if(objectConditions.isEmpty()){
            System.out.println("Empty Object Conditions");
        }
        return new BEPolicy(policyID, objectConditions, querierConditions, "analysis",
                action, new Timestamp(System.currentTimeMillis()));
    }
}
