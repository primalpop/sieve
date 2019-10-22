package edu.uci.ics.tippers.manager;

import edu.uci.ics.tippers.db.PGSQLConnectionManager;
import edu.uci.ics.tippers.model.policy.BEPolicy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

public class FlatPolicyPersistor {

    private static FlatPolicyPersistor _instance = new FlatPolicyPersistor();

    private static Connection connection = PGSQLConnectionManager.getInstance().getConnection();

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
                    "(id, querier, owner, purpose, enforcement_action, inserted_at, uEq, " +
                    "lEq, sb, se, fb, fe) VALUES (?, ?, ?, ?, ?, ?, ?," +
                    "?, ?, ?, ?, ?, ?)";
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
                List<Timestamp> start = bePolicy.fetchStart();
                policyStmt.setTimestamp(9, start.get(0));
                policyStmt.setTimestamp(10, start.get(1));
                List<Timestamp> finish = bePolicy.fetchFinish();
                policyStmt.setTimestamp(11, finish.get(0));
                policyStmt.setTimestamp(12, finish.get(1));

                policyStmt.addBatch();
            }
            policyStmt.executeBatch();
            policyStmt.close();
        } catch(SQLException e){
            e.printStackTrace();
        }
    }
}
