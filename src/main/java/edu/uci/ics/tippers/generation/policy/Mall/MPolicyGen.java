package edu.uci.ics.tippers.generation.policy.Mall;

import edu.uci.ics.tippers.common.AttributeType;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.model.policy.*;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;

public class MPolicyGen {

    private Connection connection;
    private Random r;

    List<Integer> devices;
    List<Integer> wifi_aps;
    int shop_types;
    private LocalDate startDate = null, endDate = null;
    private LocalTime startTime = null, endTime = null;

    public MPolicyGen(){
        connection = MySQLConnectionManager.getInstance().getConnection();
        r = new Random();
    }

    public List<Integer> retrieveAllDevices() {
        PreparedStatement queryStm = null;
        devices = new ArrayList<>();
        try {
            queryStm = connection.prepareStatement("SELECT distinct device_id as id FROM MALL_OBSERVATION order by device_id");
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) devices.add(rs.getInt("id"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return devices;
    }

    public List<Integer> retrieveAllWiFiAPs() {
        PreparedStatement queryStm = null;
        wifi_aps = new ArrayList<>();
        try {
            queryStm = connection.prepareStatement("SELECT distinct wifi_ap as id FROM MALL_OBSERVATION order by wifi_ap");
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) wifi_aps.add(rs.getInt("id"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return wifi_aps;
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

    public LocalDate getObsDate(String valType){
        if (valType.equalsIgnoreCase("MIN")) {
            if(startDate == null) startDate = retrieveDate(valType);
            return startDate;
        } else {
            if(endDate == null) endDate = retrieveDate(valType);
            return endDate;
        }
    }




    /**
     * @param owner_id - id of the owner of the tuple
     * @param querier - type of shop/wifi_ap
     * @param tsPred - time period captured using date and time
     * @param action - allow or deny
     * @return
     */
    public BEPolicy generatePolicies(int querier, int owner_id, TimeStampPredicate tsPred, String action) {
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
        if (tsPred != null) {
            ObjectCondition datePred = new ObjectCondition(policyID, PolicyConstants.START_DATE, AttributeType.DATE,
                    tsPred.getStartDate().toString(), Operation.GTE, tsPred.getEndDate().toString(), Operation.LTE);
            objectConditions.add(datePred);
            ObjectCondition timePred = new ObjectCondition(policyID, PolicyConstants.START_TIME, AttributeType.TIME,
                    tsPred.parseStartTime(), Operation.GTE, tsPred.parseEndTime(), Operation.LTE);
            objectConditions.add(timePred);
        }
        if(objectConditions.isEmpty()){
            System.out.println("Empty Object Conditions");
        }
        return new BEPolicy(policyID, objectConditions, querierConditions, "analysis",
                action, new Timestamp(System.currentTimeMillis()));
    }
}
