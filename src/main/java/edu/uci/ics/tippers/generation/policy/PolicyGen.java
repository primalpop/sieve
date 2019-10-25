package edu.uci.ics.tippers.generation.policy;

import edu.uci.ics.tippers.common.AttributeType;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.model.policy.*;

import java.sql.*;
import java.util.*;

public class PolicyGen {

    /**
     * 1. Read all distinct user ids from database and store them in USER TABLE if it doesn't already exist
     * 2. Read all distinct locations from database and store them in LOCATION TABLE if it doesn't already exist
     * 3. Read smallest and largest values for start and end timestamps
     */

    private Connection connection;
    private Random r;
    private List<Integer> user_ids;
    private List<String> location_ids;
    public Timestamp start_beg;
    public Timestamp end_fin;

    public PolicyGen(){
       r = new Random();
       connection = MySQLConnectionManager.getInstance().getConnection();
       start_beg = getTimestamp("start", "MIN"); //TODO: to remove
       end_fin = getTimestamp("finish", "MAX"); //TODO: to remove
    }


    public List<Integer> getAllUsers() {
        PreparedStatement queryStm = null;
        user_ids = new ArrayList<>();
        try {
            queryStm = connection.prepareStatement("SELECT ID as id " +
                    "FROM USER");
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) user_ids.add(rs.getInt("id"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return user_ids;
    }

    public List<String> getAllLocations() {
        PreparedStatement queryStm = null;
        location_ids = new ArrayList<>();
        try {
            queryStm = connection.prepareStatement("SELECT NAME as room " +
                    "FROM LOCATION");
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) location_ids.add(rs.getString("room"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return location_ids;
    }

    public Timestamp getTimestamp(String colName, String valType){
        PreparedStatement queryStm = null;
        Timestamp ts = null;
        try{
            queryStm = connection.prepareStatement("SELECT " + valType + "(" + colName + ") AS value from PRESENCE");
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) ts = rs.getTimestamp("value");
        } catch(SQLException e) {
            e.printStackTrace();
        }
        return ts;
    }


    /**
     * @param querier - id of the querier to whom policy applies
     * @param owner_id - id of the owner of the tuple
     * @param tsPred - time period captured using date and time
     * @param location - location value
     * @param action   - allow or deny
     * @return
     */
    public List<BEPolicy> generatePolicies(int owner_id, int querier, TimeStampPredicate tsPred, String location, String action) {
        List<BEPolicy> bePolicies = new ArrayList<>();
        String policyID = UUID.randomUUID().toString();
        List<QuerierCondition> querierConditions = new ArrayList<>(Arrays.asList(
                new QuerierCondition(policyID, "policy_type", AttributeType.STRING, Operation.EQ, "user"),
                new QuerierCondition(policyID, "querier", AttributeType.STRING, Operation.EQ, String.valueOf(querier))));
        List<ObjectCondition> objectConditions = new ArrayList<>();
        ObjectCondition ownerPred = new ObjectCondition(policyID, PolicyConstants.USERID_ATTR, AttributeType.STRING,
                String.valueOf(owner_id), Operation.EQ);
        objectConditions.add(ownerPred);
        if (tsPred != null) {
            ObjectCondition datePred = new ObjectCondition(policyID, PolicyConstants.START_DATE, AttributeType.DATE,
                    tsPred.getStartDate().toString(), Operation.GTE, tsPred.getEndDate().toString(), Operation.LTE);
            objectConditions.add(datePred);
            ObjectCondition timePred = new ObjectCondition(policyID, PolicyConstants.START_TIME, AttributeType.TIME,
                    tsPred.parseStartTime(), Operation.GTE, tsPred.parseEndTime(), Operation.LTE);
            objectConditions.add(timePred);
        }
        if (location != null) {
            ObjectCondition locationPred = new ObjectCondition(policyID, PolicyConstants.LOCATIONID_ATTR, AttributeType.STRING,
                    location, Operation.EQ);
            objectConditions.add(locationPred);
        }
        bePolicies.add(new BEPolicy(policyID, objectConditions, querierConditions, "analysis",
                action, new Timestamp(System.currentTimeMillis())));
        return bePolicies;
    }

}
