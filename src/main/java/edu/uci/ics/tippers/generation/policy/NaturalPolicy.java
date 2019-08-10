package edu.uci.ics.tippers.generation.policy;


import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.generation.data.DataGeneration;
import edu.uci.ics.tippers.model.data.User;
import edu.uci.ics.tippers.model.data.UserGroup;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.tippers.Infrastructure;

import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Random;

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
            queryStm = connection.prepareStatement("SELECT u.user_type, u.totalTime, u.ID, max(c) " +
                    "FROM (SELECT u.user_type, u.totalTime, u.ID, count(*) as c FROM USER as u, USER_GROUP_MEMBERSHIP as ugm " +
                    "where u.ID = ugm.USER_ID group by u.ID)");
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) {
                this.querier.setUserId(Integer.parseInt(rs.getString("u.ID")));
                this.querier.setTotalTime(rs.getInt("u.totalTime"));
                this.querier.setUserType(rs.getString("u.user_type"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        this.querier.retrieveUserGroups();
    }

    private long getRandomTimeBetweenTwoDates () {
        long diff = Timestamp.valueOf(PolicyConstants.END_TS).getTime() -
                Timestamp.valueOf(PolicyConstants.START_TS).getTime() + 1;
        return Timestamp.valueOf(PolicyConstants.START_TS).getTime() + (long) (Math.random() * diff);
    }

    private Timestamp getRandomTimeStamp() {
        LocalDateTime randomDate = Instant.ofEpochMilli(getRandomTimeBetweenTwoDates()).atZone(ZoneId.systemDefault()).toLocalDateTime();
        return Timestamp.valueOf(randomDate);
    }

    private Timestamp getEndingTimeInterval(Timestamp timestamp){
        if (timestamp == null)
            return getRandomTimeStamp();
        int hourIndex = new Random().nextInt(PolicyConstants.HOUR_EXTENSIONS.size());
        double rHour = PolicyConstants.HOUR_EXTENSIONS.get(hourIndex);

        rHour = rHour * Math.random();
        Long milliseconds = (long)(rHour * 60.0 * 60.0 * 1000.0);
        return new Timestamp(timestamp.getTime() + milliseconds);
    }




    public void generatePolicies(User querier){
        for (UserGroup ug: querier.getGroups()) {
            for (User u: ug.getMembers()) {
                /**
                 * Create a user object condition
                 * Create a location object condition
                 * Create a start and end timestamp condition
                 * Create the policy
                 * Create the querier
                 */

            }
        }
    }


}
