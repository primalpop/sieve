package edu.uci.ics.tippers.manager;

import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.model.policy.BEPolicy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class SimplePersistor {

    private static SimplePersistor _instance = new SimplePersistor();

    private static Connection connection = MySQLConnectionManager.getInstance().getConnection();

    public static SimplePersistor getInstance() {
        return _instance;
    }

    /**
     * @param bePolicyList
     */
    public void insertPolicies(List<BEPolicy> bePolicyList) {

        try {
            connection.setAutoCommit(true);

            String policyInsert = "INSERT INTO SIMPLE_POLICY " +
                    "(id, querier, owner, purpose, enforcement_action, inserted_at, uPol, " +
                    "lPol, templPol, tempgPol, elPol, egPol, aPol, tslPol, tsgPol) VALUES (?, ?, ?, ?, ?, ?, ?," +
                    "?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement policyStmt = connection.prepareStatement(policyInsert);

            for (BEPolicy bePolicy : bePolicyList) {
                policyStmt.setString(1, bePolicy.getId());
                policyStmt.setString(2, bePolicy.fetchQuerier());
                policyStmt.setString(3, bePolicy.fetchOwner());
                policyStmt.setString(4, bePolicy.getPurpose());
                policyStmt.setString(5, bePolicy.getAction());
                policyStmt.setTimestamp(6, bePolicy.getInserted_at());
                policyStmt.setString(7, bePolicy.fetchOwner());
                policyStmt.setString(8, bePolicy.fetchLocation());
                policyStmt.setString(9, bePolicy.fetchTemperatureLowValue());
                policyStmt.setString(10, bePolicy.fetchTemperatureHighValue());
                policyStmt.setString( 11, bePolicy.fetchEnergyLowValue());
                policyStmt.setString( 12, bePolicy.fetchEnergyHighValue());
                policyStmt.setString( 13, bePolicy.fetchActivity());
                policyStmt.setTimestamp( 14, bePolicy.fetchTimestampLowValue());
                policyStmt.setTimestamp( 15, bePolicy.fetchTimestampHighValue());
                policyStmt.addBatch();
            }
            policyStmt.executeBatch();
            policyStmt.close();
        } catch(SQLException e){
            e.printStackTrace();
        }
    }
}
