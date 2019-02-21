package edu.uci.ics.tippers.manager;

import edu.uci.ics.tippers.common.AttributeType;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.model.data.User;
import edu.uci.ics.tippers.model.data.UserGroup;
import edu.uci.ics.tippers.model.policy.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

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
    public void insertPolicy(BEPolicy bePolicy) {

        try{

            if(bePolicy.isUserPolicy()){ //User Policy

                String userPolicyInsert = "INSERT INTO USER_POLICY " +
                        "(id, querier, purpose, enforcement_action, inserted_at) VALUES (?, ?, ?, ?, ?)";

                PreparedStatement userPolicyStmt = connection.prepareStatement(userPolicyInsert);
                userPolicyStmt.setString(1, bePolicy.getId());
                userPolicyStmt.setInt(2, Integer.parseInt(bePolicy.getQuerier()));
                userPolicyStmt.setString(3, bePolicy.getPurpose());
                userPolicyStmt.setString(4, bePolicy.getAction());
                userPolicyStmt.setTimestamp(5, bePolicy.getInserted_at());
                userPolicyStmt.executeUpdate();
                userPolicyStmt.close();

                String objectConditionInsert = "INSERT INTO USER_POLICY_OBJECT_CONDITION " +
                        "(policy_id, attribute, attribute_type, operator, comp_value) VALUES (?, ?, ?, ?, ?)";

                PreparedStatement ocStmt = connection.prepareStatement(objectConditionInsert);
                for (ObjectCondition oc: bePolicy.getObject_conditions()) {
                    for (BooleanPredicate bp: oc.getBooleanPredicates()) {
                        ocStmt.setString(1, bePolicy.getId());
                        ocStmt.setString(2, oc.getAttribute());
                        ocStmt.setString(3, oc.getType().toString());
                        ocStmt.setString(4, bp.getOperator());
                        ocStmt.setString(5, bp.getValue());
                        ocStmt.addBatch();
                    }
                }
                ocStmt.executeBatch();
            }
            else { //Group Policy

                String groupPolicyInsert = "INSERT INTO GROUP_POLICY " +
                        "(id, querier, purpose, enforcement_action, inserted_at) VALUES (?, ?, ?, ?, ?)";

                PreparedStatement groupPolicyStmt = connection.prepareStatement(groupPolicyInsert);

                groupPolicyStmt.setString(1, bePolicy.getId());
                groupPolicyStmt.setInt(2, Integer.parseInt(bePolicy.getQuerier()));
                groupPolicyStmt.setString(3, bePolicy.getPurpose());
                groupPolicyStmt.setString(4, bePolicy.getAction());
                groupPolicyStmt.setTimestamp(5, bePolicy.getInserted_at());
                groupPolicyStmt.executeUpdate();
                groupPolicyStmt.close();

                String objectConditionInsert = "INSERT INTO GROUP_POLICY_OBJECT_CONDITION " +
                        "(policy_id, attribute, attribute_type, operator, comp_value) VALUES (?, ?, ?, ?, ?)";

                PreparedStatement ocStmt = connection.prepareStatement(objectConditionInsert);
                for (ObjectCondition oc: bePolicy.getObject_conditions()) {
                    for (BooleanPredicate bp: oc.getBooleanPredicates()) {
                        ocStmt.setString(1, bePolicy.getId());
                        ocStmt.setString(2, oc.getAttribute());
                        ocStmt.setString(3, oc.getType().toString());
                        ocStmt.setString(4, bp.getOperator());
                        ocStmt.setString(5, bp.getValue());
                        ocStmt.addBatch();
                    }
                }
                ocStmt.executeBatch();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public BEPolicy retrievePolicy(String querier, String querier_type) {
        BEPolicy bePolicy = new BEPolicy();
        String id = null, purpose = null, action = null;
        Timestamp inserted_at = null;
        List<ObjectCondition> objectConditions = new ArrayList<>();
        List<QuerierCondition> querierConditions = new ArrayList<>();
        if(querier_type.equalsIgnoreCase("user")){
            try {
                PreparedStatement usrStm = connection.prepareStatement("SELECT up.id, up.querier, up.purpose, " +
                        "up.enforcement_action, up.inserted_at, upo.id, upo.policy_id, upo.attribute, upo.attribute_type, " +
                        "upo.operator, upo.comp_value FROM USER_POLICY as up, " +
                        "USER_POLICY_OBJECT_CONDITION as upo where up.querier=? and up.id = upo.policy_id");
                usrStm.setInt(1, Integer.parseInt(querier));
                ResultSet rs = usrStm.executeQuery();
                while(rs.next()){
                    if (id == null){
                        id = rs.getString("up.id");
                        purpose = rs.getString("up.purpose");
                        action = rs.getString("up.enforcement_action");
                        inserted_at = rs.getTimestamp("up.inserted_at");

                        QuerierCondition qc1 = new QuerierCondition();
                        qc1.setAttribute("policy_type");
                        qc1.setType(AttributeType.STRING);
                        List<BooleanPredicate> qbps1 = new ArrayList<>();
                        BooleanPredicate bp1 = new BooleanPredicate();
                        bp1.setOperator("=");
                        bp1.setValue(querier_type);
                        qbps1.add(bp1);
                        qc1.setBooleanPredicates(qbps1);
                        querierConditions.add(qc1);

                        QuerierCondition qc2 = new QuerierCondition();
                        qc2.setAttribute("querier");
                        qc2.setType(AttributeType.STRING);
                        List<BooleanPredicate> qbps2 = new ArrayList<>();
                        BooleanPredicate bp2 = new BooleanPredicate();
                        bp2.setOperator("=");
                        bp2.setValue(querier);
                        qbps2.add(bp2);
                        qc2.setBooleanPredicates(qbps2);
                        querierConditions.add(qc2);

                    }
                    ObjectCondition oc = new ObjectCondition();
                    oc.setAttribute(rs.getString("upo.attribute"));
                    oc.setPolicy_id(rs.getString("upo.policy_id"));
                    oc.setType(AttributeType.get(rs.getString("upo.attribute_type")));
                    List<BooleanPredicate> booleanPredicates = new ArrayList<>();
                    BooleanPredicate bp1 = new BooleanPredicate();
                    bp1.setOperator(rs.getString("upo.operator"));
                    bp1.setValue(rs.getString("upo.comp_value"));
                    rs.next();
                    BooleanPredicate bp2 = new BooleanPredicate();
                    bp2.setOperator(rs.getString("upo.operator"));
                    bp2.setValue(rs.getString("upo.comp_value"));
                    booleanPredicates.add(bp1);
                    booleanPredicates.add(bp2);
                    oc.setBooleanPredicates(booleanPredicates);
                    objectConditions.add(oc);
                }
                bePolicy = new BEPolicy(id, objectConditions, querierConditions, purpose, action, inserted_at);
                return bePolicy;
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
        else if(querier_type.equalsIgnoreCase("group")){
            return null;
        }
        System.out.println("Unknown Querier type");
        return null;

    }

}
