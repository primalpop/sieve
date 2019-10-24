package edu.uci.ics.tippers.generation.policy;

import edu.uci.ics.tippers.common.AttributeType;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;
import edu.uci.ics.tippers.model.policy.Operation;
import edu.uci.ics.tippers.model.policy.QuerierCondition;

import java.sql.*;
import java.text.SimpleDateFormat;
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
    public Timestamp start_beg, start_fin;
    public Timestamp end_beg, end_fin;

    public PolicyGen(){
       r = new Random();
       this.connection = MySQLConnectionManager.getInstance().getConnection();
       this.start_beg = getTimestamp(PolicyConstants.START_TIMESTAMP_ATTR, "MIN");
       this.start_fin = getTimestamp(PolicyConstants.START_TIMESTAMP_ATTR, "MAX");
       this.end_beg = getTimestamp(PolicyConstants.FINISH_TIMESTAMP_ATTR, "MIN");
       this.end_fin = getTimestamp(PolicyConstants.FINISH_TIMESTAMP_ATTR, "MAX");
    }


    public List<Integer> getAllUsers() {
        PreparedStatement queryStm = null;
        user_ids = new ArrayList<>();
        try {
            queryStm = connection.prepareStatement("SELECT ID as id " +
                    "FROM dummy_user");
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
     * Beginning from first timestamp create timestamps for each day based on the time periods specified in periods
     * @return
     */
    public List<TimeStampPredicate> generateTsWith(int start, int duration){
        long timeStampOffset = 60000;
        List<TimeStampPredicate> tsPreds = new ArrayList<>();
        Calendar startCal = Calendar.getInstance();
        startCal.setTime(start_beg);
        startCal.set(Calendar.HOUR_OF_DAY, start);
        Calendar finCal = Calendar.getInstance();
        finCal.setTime(startCal.getTime());
        finCal.add(Calendar.HOUR_OF_DAY, duration);
        Calendar startEnd = Calendar.getInstance();
        startEnd.setTime(start_fin);
        Calendar FinEnd = Calendar.getInstance();
        FinEnd.setTime(end_fin);
        while(startEnd.compareTo(startCal) > 0 && FinEnd.compareTo(finCal) > 0){
            Timestamp cStartBegTs = new Timestamp(startCal.getTime().getTime());
            Timestamp cStartEndTs = new Timestamp(startCal.getTime().getTime() + timeStampOffset);
            Timestamp cFinBegTs = new Timestamp(finCal.getTime().getTime());
            Timestamp cFinEndTs = new Timestamp(finCal.getTime().getTime() + timeStampOffset);
            TimeStampPredicate timeStampPredicate = new TimeStampPredicate(cStartBegTs, cStartEndTs, cFinBegTs, cFinEndTs);
            tsPreds.add(timeStampPredicate);
            startCal.add(Calendar.DAY_OF_MONTH, 1);
            finCal.add(Calendar.DAY_OF_MONTH, 1);
        }
        return tsPreds;
    }


    /**
     * @param querier
     * @param owner_id
     * @param start - starting time of the period
     * @param duration - length of the period
     * @param location - location value
     * @param action - allow or deny
     * @return
     */
    public List<BEPolicy> generatePolicies(int querier, int owner_id, int start, int duration, String location, String action){
        List<BEPolicy> bePolicies = new ArrayList<>();
        List<TimeStampPredicate> tsPreds = generateTsWith(start, duration);
        String policyID = UUID.randomUUID().toString();
        for (TimeStampPredicate tsPred: tsPreds) {
            List<QuerierCondition> querierConditions = new ArrayList<>(Arrays.asList(
                    new QuerierCondition(policyID, "policy_type", AttributeType.STRING, Operation.EQ, "user"),
                    new QuerierCondition(policyID, "querier", AttributeType.STRING, Operation.EQ, String.valueOf(querier))));
            List<ObjectCondition> objectConditions = new ArrayList<>();
            ObjectCondition ownerPred = new ObjectCondition(policyID, PolicyConstants.USERID_ATTR, AttributeType.STRING,
                    String.valueOf(owner_id), Operation.EQ);
            objectConditions.add(ownerPred);
            if(start != 0 && duration != 0){
                SimpleDateFormat sdf = new SimpleDateFormat(PolicyConstants.TIMESTAMP_FORMAT);
                ObjectCondition tp_beg = new ObjectCondition(policyID, PolicyConstants.START_TIMESTAMP_ATTR, AttributeType.TIMESTAMP,
                        sdf.format(tsPred.getStartBeg()), Operation.GTE, sdf.format(tsPred.getStartEnd()), Operation.LTE);
                objectConditions.add(tp_beg);
                ObjectCondition tp_end = new ObjectCondition(policyID, PolicyConstants.FINISH_TIMESTAMP_ATTR, AttributeType.TIMESTAMP,
                        sdf.format(tsPred.getFinBeg()), Operation.GTE, sdf.format(tsPred.getFinEnd()), Operation.LTE);
                objectConditions.add(tp_end);
            }
            if (location != null) {
                ObjectCondition locationPred = new ObjectCondition(policyID, PolicyConstants.LOCATIONID_ATTR, AttributeType.STRING,
                        location, Operation.EQ);
                objectConditions.add(locationPred);
            }
            bePolicies.add(new BEPolicy(policyID, objectConditions, querierConditions, "analysis",
                    action, new Timestamp(System.currentTimeMillis())));
        }
        return bePolicies;
    }

    class TimeStampPredicate{

        Timestamp startBeg;
        Timestamp startEnd;
        Timestamp finBeg;
        Timestamp finEnd;


        public TimeStampPredicate(Timestamp cStartBegTs, Timestamp cStartEndTs, Timestamp cFinBegTs, Timestamp cFinEndTs) {
            this.startBeg = cStartBegTs;
            this.startEnd = cStartEndTs;
            this.finBeg = cFinBegTs;
            this.finEnd = cFinEndTs;
        }

        public Timestamp getStartBeg() {
            return startBeg;
        }

        public void setStartBeg(Timestamp startBeg) {
            this.startBeg = startBeg;
        }

        public Timestamp getStartEnd() {
            return startEnd;
        }

        public void setStartEnd(Timestamp startEnd) {
            this.startEnd = startEnd;
        }

        public Timestamp getFinBeg() {
            return finBeg;
        }

        public void setFinBeg(Timestamp finBeg) {
            this.finBeg = finBeg;
        }

        public Timestamp getFinEnd() {
            return finEnd;
        }

        public void setFinEnd(Timestamp finEnd) {
            this.finEnd = finEnd;
        }
    }
}
