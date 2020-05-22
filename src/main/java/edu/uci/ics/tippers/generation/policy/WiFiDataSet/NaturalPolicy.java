package edu.uci.ics.tippers.generation.policy.WiFiDataSet;


import edu.uci.ics.tippers.common.AttributeType;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.generation.data.DataGeneration;
import edu.uci.ics.tippers.model.data.User;
import edu.uci.ics.tippers.model.data.UserGroup;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;
import edu.uci.ics.tippers.model.policy.Operation;
import edu.uci.ics.tippers.model.policy.QuerierCondition;
import edu.uci.ics.tippers.model.tippers.dontuse.Infrastructure;

import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

/**
 * 1. Select a querier with large number of group_memberships
 * 2. Select the groups querier belongs to
 * 3. For each group, select x members from that group and create a policy
 */


public class NaturalPolicy {

    User querier;
    List<BEPolicy> policies;
    int numberOfPolicies;
    List<Infrastructure> infras = DataGeneration.getAllInfra();

    private static Connection connection = MySQLConnectionManager.getInstance().getConnection();

    //Get the user with maximum number of group memberships
    public void getQuerier() {
        PreparedStatement queryStm = null;
        try {
            queryStm = connection.prepareStatement("SELECT u.user_type, max(c) " +
                    "FROM (SELECT u.user_type, count(*) as c FROM USER as u, USER_GROUP_MEMBERSHIP as ugm " +
                    "where u.ID = ugm.USER_ID group by u.ID)");
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) {
                this.querier.setUserId(Integer.parseInt(rs.getString("u.ID")));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        this.querier.retrieveUserGroups();
    }

    private long getRandomTimeBetweenTwoDates() {
        long diff = Timestamp.valueOf(PolicyConstants.END_TS).getTime() -
                Timestamp.valueOf(PolicyConstants.START_TS).getTime() + 1;
        return Timestamp.valueOf(PolicyConstants.START_TS).getTime() + (long) (Math.random() * diff);
    }

    private Timestamp getRandomTimeStamp() {
        LocalDateTime randomDate = Instant.ofEpochMilli(getRandomTimeBetweenTwoDates()).atZone(ZoneId.systemDefault()).toLocalDateTime();
        return Timestamp.valueOf(randomDate);
    }

    private Timestamp getEndingTimeInterval(Timestamp timestamp) {
        if (timestamp == null)
            return getRandomTimeStamp();
        int hourIndex = new Random().nextInt(PolicyConstants.HOUR_EXTENSIONS.size());
        double rHour = PolicyConstants.HOUR_EXTENSIONS.get(hourIndex);

        rHour = rHour * Math.random();
        Long milliseconds = (long) (rHour * 60.0 * 60.0 * 1000.0);
        return new Timestamp(timestamp.getTime() + milliseconds);
    }

    private List<ObjectCondition> createObjectConditions(String policyID, String userID, String locationID,
                                                  Timestamp startTimestamp, Timestamp endTimestamp){
        List<ObjectCondition> objectConditions = new ArrayList<>();
//        objectConditions.add(new ObjectCondition(policyID, "user_id", AttributeType.STRING,
//                "=", userID, "=", userID));
//        objectConditions.add(new ObjectCondition(policyID, "location_id",
//                AttributeType.STRING, "=", locationID, "=", locationID));
//        SimpleDateFormat sdf = new SimpleDateFormat(PolicyConstants.TIMESTAMP_FORMAT);
//        objectConditions.add(new ObjectCondition(policyID, "timeStamp", AttributeType.TIMESTAMP,
//                ">=", sdf.format(startTimestamp), "<=", sdf.format(endTimestamp)));
        return objectConditions;
    }


    /**
     * For the given querier, creates policies based on their group memberships
     * i.e for each they are a member of, for each member create a policy
     * @param querier
     */
    public List<BEPolicy> generateGroupMemberPolicies(User querier) {
        List<BEPolicy> policyList = new ArrayList<>();
        for (UserGroup ug : querier.getGroups()) {
            for (User u : ug.getMembers()) {
                String policyID = UUID.randomUUID().toString();
                BEPolicy groupMemPolicy = new BEPolicy();
                List<QuerierCondition> querierConditions = new ArrayList<>(Arrays.asList(
                        new QuerierCondition(policyID, "policy_type", AttributeType.STRING, Operation.EQ,"user"),
                        new QuerierCondition(policyID, "querier", AttributeType.STRING, Operation.EQ,
                                String.valueOf(querier.getUserId()))));
                groupMemPolicy.setQuerier_conditions(querierConditions);
                groupMemPolicy.setAction("allow");
                groupMemPolicy.setPurpose("application");
                groupMemPolicy.setInserted_at(new Timestamp(System.currentTimeMillis()));
                groupMemPolicy.setId(policyID);
                String location_id = infras.get(new Random().nextInt(infras.size())).getName();
                Timestamp start_timestamp = getRandomTimeStamp();
                Timestamp end_timestamp = getEndingTimeInterval(start_timestamp);
                groupMemPolicy.setObject_conditions(createObjectConditions(policyID, String.valueOf(u.getUserId()), location_id,
                        start_timestamp, end_timestamp));
                policyList.add(groupMemPolicy);
            }
        }
        return policyList;
    }

    /**
     * Generate policies for a list of groups!
     * Some of these groups are user's own groups
     * Others are non-groups
     */
    public List<BEPolicy> generateGroupPolicies(List<UserGroup> userGroups){
        return null;
    }


    /**
     * Generate policies for list of users
     * list of users can come from a group
     * or just random list of users (e.g. when creating policy for a role or region or floor)
     */
    public List<BEPolicy> generateUserPolicies(List<User> users){
        return null;
    }

    /**
     * Query Generation
     * Review current one and check the template. What changes need to be done for the new version?
     */

    public void generateQuery(){

    }

}

