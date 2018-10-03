package edu.uci.ics.tippers.manager;

import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.model.data.User;
import edu.uci.ics.tippers.model.data.UserGroup;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.BooleanPredicate;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Persistor {

    private static Persistor _instance = new Persistor();

    private static Connection connection = MySQLConnectionManager.getInstance().getConnection();

    public static Persistor getInstance() {
        return _instance;
    }

    /**
     * Inserts a policy into a relational table based on whether it's a user policy or a group policy
     * @param bePolicy
     */
    public void insertPolicy(BEPolicy bePolicy){

        try{

            if(bePolicy.isUserPolicy()){ //User Policy

                User querier = User.class.cast(bePolicy.getPolicy_subject());

                String userPolicyInsert = "INSERT INTO USER_POLICY " +
                        "(querier, purpose, enforcement_action, inserted_at) VALUES (?, ?, ?, ?)";

                PreparedStatement userPolicyStmt = connection.prepareStatement(userPolicyInsert);

                userPolicyStmt.setInt(1, querier.getUser_id());
                userPolicyStmt.setString(2, bePolicy.getPurpose());
                userPolicyStmt.setString(3, bePolicy.getAction());
                userPolicyStmt.setTimestamp(4, bePolicy.getInserted_at());
                int policy_id = userPolicyStmt.executeUpdate();
                bePolicy.setId(policy_id);
                userPolicyStmt.close();

                String objectConditionInsert = "INSERT INTO USER_POLICY_OBJECT_CONDITION " +
                        "(policy_id, attribute, operator, comp_value) VALUES (?, ?, ?, ?)";

                PreparedStatement ocStmt = connection.prepareStatement(objectConditionInsert);
                for (ObjectCondition oc: bePolicy.getObject_conditions()) {
                    for (BooleanPredicate bp: oc.getBooleanPredicates()) {
                        ocStmt.setInt(1, bePolicy.getId());
                        ocStmt.setString(2, oc.getAttribute());
                        ocStmt.setString(3, bp.getOperator());
                        ocStmt.setString(4, bp.getValue());
                        ocStmt.addBatch();
                    }
                }
                ocStmt.executeBatch();
            }
            else { //Group Policy

                UserGroup querierGroup = UserGroup.class.cast(bePolicy.getPolicy_subject());

                String groupPolicyInsert = "INSERT INTO GROUP_POLICY " +
                        "(querier, purpose, enforcement_action, inserted_at) VALUES (?, ?, ?, ?)";

                PreparedStatement groupPolicyStmt = connection.prepareStatement(groupPolicyInsert);

                groupPolicyStmt.setInt(1, querierGroup.getGroup_id());
                groupPolicyStmt.setString(2, bePolicy.getPurpose());
                groupPolicyStmt.setString(3, bePolicy.getAction());
                groupPolicyStmt.setTimestamp(4, bePolicy.getInserted_at());
                int policy_id = groupPolicyStmt.executeUpdate();
                bePolicy.setId(policy_id);
                groupPolicyStmt.close();

                String objectConditionInsert = "INSERT INTO GROUP_POLICY_OBJECT_CONDITION " +
                        "(policy_id, attribute, operator, comp_value) VALUES (?, ?, ?, ?)";

                PreparedStatement ocStmt = connection.prepareStatement(objectConditionInsert);
                for (ObjectCondition oc: bePolicy.getObject_conditions()) {
                    for (BooleanPredicate bp: oc.getBooleanPredicates()) {
                        ocStmt.setInt(1, bePolicy.getId());
                        ocStmt.setString(2, oc.getAttribute());
                        ocStmt.setString(3, bp.getOperator());
                        ocStmt.setString(4, bp.getValue());
                        ocStmt.addBatch();
                    }
                }
                ocStmt.executeBatch();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }



}
