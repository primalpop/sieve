package edu.uci.ics.tippers.generation.policy.WiFiDataSet;

import edu.uci.ics.tippers.common.AttributeType;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.dbms.mysql.MySQLConnectionManager;
import edu.uci.ics.tippers.dbms.postgresql.PGSQLConnectionManager;
import edu.uci.ics.tippers.model.data.UserProfile;
import edu.uci.ics.tippers.model.policy.*;

import java.sql.*;
import java.util.*;

public class PolicyUtil {

    /**
     * 1. Read all distinct user ids from database and store them in USER TABLE if it doesn't already exist
     * 2. Read all distinct locations from database and store them in LOCATION TABLE if it doesn't already exist
     * 3. Read smallest and largest values for start and end timestamps
     */

    private Connection connection;
    private Random r;
    private List<Integer> user_ids;
    private List<String> location_ids;
    private Timestamp startDate = null, endDate = null;

    public PolicyUtil() {
        r = new Random();
        if (PolicyConstants.DBMS_CHOICE.equalsIgnoreCase(PolicyConstants.MYSQL_DBMS))
            connection = MySQLConnectionManager.getInstance().getConnection();
        else if (PolicyConstants.DBMS_CHOICE.equalsIgnoreCase(PolicyConstants.PGSQL_DBMS))
            connection = PGSQLConnectionManager.getInstance().getConnection();
        else
            System.out.println("DBMS choice not set or unknown DBMS");
    }


    public List<Integer> getAllUsers(boolean non_visitor) {
        PreparedStatement queryStm = null;
        user_ids = new ArrayList<>();
        try {
            if (!non_visitor)
                queryStm = connection.prepareStatement("SELECT ID as id " +
                        "FROM APP_USER ORDER BY id");
            else
                queryStm = connection.prepareStatement("SELECT ID as id FROM APP_USER where USER_PROFILE in ( \'"
                        + UserProfile.FACULTY.getValue() + "\', \'" + UserProfile.GRADUATE.getValue()
                        + "\', \'" + UserProfile.UNDERGRAD.getValue() + "\', \'" + UserProfile.STAFF.getValue()
                        + "\') ORDER BY id");
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
                    "FROM DBH_LOCATION");
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) location_ids.add(rs.getString("room"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return location_ids;
    }

    public Timestamp retrieveDate(String valType) {
        PreparedStatement queryStm = null;
        Timestamp ts = null;
        try {
            queryStm = connection.prepareStatement("SELECT " + valType + "(" + PolicyConstants.START_DATE + ") AS value from PRESENCE");
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) ts = rs.getTimestamp("value");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ts;
    }

    public Timestamp getDate(String valType) {
        if (valType.equalsIgnoreCase("MIN")) {
            if (startDate == null) startDate = retrieveDate(valType);
            return startDate;
        } else {
            if (endDate == null) endDate = retrieveDate(valType);
            return endDate;
        }
    }


    /**
     * @param owner_id - id of the owner of the tuple
     * @param querier  - id of the querier to whom policy applies
     * @param tsPred   - time period captured using date and time
     * @param location - location value
     * @param action   - allow or deny
     * @return
     */
    public BEPolicy generatePolicies(int querier, int owner_id, String owner_group, String owner_profile,
                                     TimeStampPredicate tsPred, String location, String action) {
        String policyID = UUID.randomUUID().toString();
        List<QuerierCondition> querierConditions = new ArrayList<>(Arrays.asList(
                new QuerierCondition(policyID, "policy_type", AttributeType.STRING, Operation.EQ, "user"),
                new QuerierCondition(policyID, "querier", AttributeType.STRING, Operation.EQ, String.valueOf(querier))));
        List<ObjectCondition> objectConditions = new ArrayList<>();
        if (owner_id != 0) {
            ObjectCondition owner = new ObjectCondition(policyID, PolicyConstants.USERID_ATTR, AttributeType.STRING,
                    String.valueOf(owner_id), Operation.EQ);
            objectConditions.add(owner);
        }
        if (owner_group != null) {
            ObjectCondition ownerGroup = new ObjectCondition(policyID, PolicyConstants.GROUP_ATTR, AttributeType.STRING,
                    owner_group, Operation.EQ);
            objectConditions.add(ownerGroup);
        }
        if (owner_profile != null) {
            ObjectCondition ownerProfile = new ObjectCondition(policyID, PolicyConstants.PROFILE_ATTR, AttributeType.STRING,
                    owner_profile, Operation.EQ);
            objectConditions.add(ownerProfile);
        }
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
        if (objectConditions.isEmpty()) {
            System.out.println("Empty Object Conditions");
        }
        return new BEPolicy(policyID, objectConditions, querierConditions, "analysis",
                action, new Timestamp(System.currentTimeMillis()));
    }
}
