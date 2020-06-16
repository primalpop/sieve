package edu.uci.ics.tippers.persistor;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.dbms.mysql.MySQLConnectionManager;
import edu.uci.ics.tippers.model.policy.BEPolicy;

import java.sql.*;
import java.util.List;

public class FlatPolicyPersistor {

    private static FlatPolicyPersistor _instance = new FlatPolicyPersistor();

    private static Connection connection = MySQLConnectionManager.getInstance().getConnection();

    public static FlatPolicyPersistor getInstance() {
        return _instance;
    }

    /**
     * @param bePolicyList
     */
    public void insertPolicies(List<BEPolicy> bePolicyList) {

        try {
            connection.setAutoCommit(true);
            String policyInsert = "INSERT INTO FLAT_POLICY " +
                    "(id, querier, purpose, enforcement_action, inserted_at, ownerEq, profEq, groupEq, " +
                    "locEq, dateGe, dateLe, timeGe, timeLe, selectivity) VALUES (?, ?, ?, ?, ?, ?, ?," +
                    "?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement policyStmt = connection.prepareStatement(policyInsert);
            for (int i = 0; i < bePolicyList.size() ; i++) {
                BEPolicy bePolicy = bePolicyList.get(i);
                policyStmt.setString(1, bePolicy.getId());
                policyStmt.setString(2, bePolicy.fetchQuerier());
                policyStmt.setString(3, bePolicy.getPurpose());
                policyStmt.setString(4, bePolicy.getAction());
                policyStmt.setTimestamp(5, bePolicy.getInserted_at());
                policyStmt.setInt(6, bePolicy.fetchOwner());
                policyStmt.setString(7, bePolicy.fetchProfile());
                policyStmt.setString(8, bePolicy.fetchGroup());
                policyStmt.setString(9, bePolicy.fetchLocation());
                List<Date> start_date = bePolicy.fetchDate();
                if(start_date.size() > 0){
                    policyStmt.setDate(10, start_date.get(0));
                    policyStmt.setDate(11, start_date.get(1));
                }
                else {
                    policyStmt.setDate(10, null);
                    policyStmt.setDate(11, null);
                }
                List<Time> start_time = bePolicy.fetchTime();
                if(start_time.size() > 0) {
                    policyStmt.setTime(12, start_time.get(0));
                    policyStmt.setTime(13, start_time.get(1));
                }
                else {
                    policyStmt.setTime(12, null);
                    policyStmt.setTime(13, null);
                }
                policyStmt.setFloat(14, bePolicy.computeL());
                policyStmt.addBatch();
            }
            policyStmt.executeBatch();
            policyStmt.close();
        } catch(SQLException e){
            e.printStackTrace();
        }
    }


    public static void main(String [] args){
        PolicyPersistor polper = PolicyPersistor.getInstance();
        FlatPolicyPersistor flapolper = new FlatPolicyPersistor();
        List<BEPolicy> allowPolicies = polper.retrievePolicies(null,
                PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
        flapolper.insertPolicies(allowPolicies);
    }
}
