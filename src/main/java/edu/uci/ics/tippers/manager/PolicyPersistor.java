package edu.uci.ics.tippers.manager;

import edu.uci.ics.tippers.common.AttributeType;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.model.policy.*;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class PolicyPersistor {

    private static PolicyPersistor _instance = new PolicyPersistor();

    private static Connection connection = MySQLConnectionManager.getInstance().getConnection();

    public static PolicyPersistor getInstance() {
        return _instance;
    }

    /**
     * Inserts a list of policies into a relational table based on whether it's a user policy or a group policy
     * @param bePolicies
     */
    public void insertPolicy(List<BEPolicy> bePolicies) {
        String userPolicyInsert = "INSERT INTO USER_POLICY " +
                "(id, querier, purpose, enforcement_action, inserted_at) VALUES (?, ?, ?, ?, ?)";
        String userobjectConditionInsert = "INSERT INTO USER_POLICY_OBJECT_CONDITION " +
                "(policy_id, attribute, attribute_type, operator, comp_value) VALUES (?, ?, ?, ?, ?)";
        String groupPolicyInsert = "INSERT INTO GROUP_POLICY " +
                "(id, querier, purpose, enforcement_action, inserted_at) VALUES (?, ?, ?, ?, ?)";
        String groupObjectConditionInsert = "INSERT INTO GROUP_POLICY_OBJECT_CONDITION " +
                "(policy_id, attribute, attribute_type, operator, comp_value) VALUES (?, ?, ?, ?, ?)";

        boolean USER_POLICY = true;

        try{
            PreparedStatement userPolicyStmt = connection.prepareStatement(userPolicyInsert);
            PreparedStatement userOcStmt = connection.prepareStatement(userobjectConditionInsert);

            PreparedStatement groupPolicyStmt = connection.prepareStatement(groupPolicyInsert);
            PreparedStatement groupOcStmt = connection.prepareStatement(groupObjectConditionInsert);

            for (BEPolicy bePolicy: bePolicies) {
                if (bePolicy.typeOfPolicy()) { //User Policy
                    userPolicyStmt.setString(1, bePolicy.getId());
                    userPolicyStmt.setInt(2, Integer.parseInt(bePolicy.fetchQuerier()));
                    userPolicyStmt.setString(3, bePolicy.getPurpose());
                    userPolicyStmt.setString(4, bePolicy.getAction());
                    userPolicyStmt.setTimestamp(5, bePolicy.getInserted_at());
                    userPolicyStmt.addBatch();

                    for (ObjectCondition oc : bePolicy.getObject_conditions()) {
                        for (BooleanPredicate bp : oc.getBooleanPredicates()) {
                            userOcStmt.setString(1, bePolicy.getId());
                            userOcStmt.setString(2, oc.getAttribute());
                            userOcStmt.setString(3, oc.getType().toString());
                            userOcStmt.setString(4, bp.getOperator().toString());
                            userOcStmt.setString(5, bp.getValue());
                            userOcStmt.addBatch();
                        }
                    }
                } else { //Group Policy
                    USER_POLICY = false;

                    groupPolicyStmt.setString(1, bePolicy.getId());
                    groupPolicyStmt.setInt(2, Integer.parseInt(bePolicy.fetchQuerier()));
                    groupPolicyStmt.setString(3, bePolicy.getPurpose());
                    groupPolicyStmt.setString(4, bePolicy.getAction());
                    groupPolicyStmt.setTimestamp(5, bePolicy.getInserted_at());
                    groupPolicyStmt.addBatch();
                    groupPolicyStmt.close();

                    for (ObjectCondition oc : bePolicy.getObject_conditions()) {
                        for (BooleanPredicate bp : oc.getBooleanPredicates()) {
                            groupOcStmt.setString(1, bePolicy.getId());
                            groupOcStmt.setString(2, oc.getAttribute());
                            groupOcStmt.setString(3, oc.getType().toString());
                            groupOcStmt.setString(4, bp.getOperator().toString());
                            groupOcStmt.setString(5, bp.getValue());
                            groupOcStmt.addBatch();
                        }
                    }
                }
                if(USER_POLICY) {
                    userPolicyStmt.executeBatch();
                    userOcStmt.executeBatch();
                }
                else {
                    groupPolicyStmt.executeBatch();
                    groupOcStmt.executeBatch();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private Operation convertOperator(String operator){
        if(operator.equalsIgnoreCase("=")) return Operation.EQ;
        else if (operator.equalsIgnoreCase(">=")) return Operation.GTE;
        else if (operator.equalsIgnoreCase("<=")) return Operation.LTE;
        else if (operator.equalsIgnoreCase("<")) return Operation.LT;
        else return Operation.GT;
    }

    public List<BEPolicy> retrievePolicies(String querier, String querier_type, String enforcement_action){
        List<BEPolicy> bePolicies = new ArrayList<>();
        String id = null, purpose = null, action = null;
        Timestamp inserted_at = null;
        List<ObjectCondition> objectConditions = new ArrayList<>();

        String policy_table = null, oc_table = null;
        if (querier_type.equalsIgnoreCase("user")) {
            policy_table = "USER_POLICY";
            oc_table = "USER_POLICY_OBJECT_CONDITION";
        } else if (querier_type.equalsIgnoreCase("group")) {
            policy_table = "GROUP_POLICY";
            oc_table = "GROUP_POLICY_OBJECT_CONDITION";
        }

        PreparedStatement queryStm = null;
        try {
            if(querier != null) {
                queryStm = connection.prepareStatement("SELECT " + policy_table + ".id, " + policy_table + ".querier, " + policy_table + ".purpose, " +
                        policy_table + ".enforcement_action," + policy_table + ".inserted_at," + oc_table + ".id, " + oc_table + " .policy_id," + oc_table + ".attribute, " +
                        oc_table + ".attribute_type, " + oc_table + ".operator," + oc_table + ".comp_value " +
                        "FROM " + policy_table + ", " + oc_table +
                        " WHERE " + policy_table + ".querier=? AND " + policy_table + ".id = " + oc_table + ".policy_id " +
                        "AND " + policy_table +  ".enforcement_action=?");
                queryStm.setInt(1, Integer.parseInt(querier));
                queryStm.setString(2, enforcement_action);
            }
            else {
                queryStm = connection.prepareStatement("SELECT " + policy_table + ".id, " + policy_table + ".querier, " + policy_table + ".purpose, " +
                        policy_table + ".enforcement_action," + policy_table + ".inserted_at," + oc_table + ".id, " + oc_table + " .policy_id," + oc_table + ".attribute, " +
                        oc_table + ".attribute_type, " + oc_table + ".operator," + oc_table + ".comp_value " +
                        "FROM " + policy_table + ", " + oc_table +
                        " WHERE " + policy_table + ".id = " + oc_table + ".policy_id " +
                        "AND " + policy_table +  ".enforcement_action=?");
                queryStm.setString(1, enforcement_action);
            }
            ResultSet rs = queryStm.executeQuery();
            if (!rs.next()) return null;
            String next = null;
            boolean skip = false;
            List<QuerierCondition> querierConditions = new ArrayList<>();
            while (true) {
                if(!skip) {
                    id = rs.getString(policy_table + ".id");
                    purpose = rs.getString(policy_table + ".purpose");
                    action = rs.getString(policy_table + ".enforcement_action");
                    inserted_at = rs.getTimestamp(policy_table + ".inserted_at");
                    querier = rs.getString(policy_table + ".querier");

                    querierConditions  = new ArrayList<>();
                    QuerierCondition qc1 = new QuerierCondition();
                    qc1.setPolicy_id(id);
                    qc1.setAttribute("policy_type");
                    qc1.setType(AttributeType.STRING);
                    List<BooleanPredicate> qbps1 = new ArrayList<>();
                    BooleanPredicate qbp1 = new BooleanPredicate();
                    qbp1.setOperator(Operation.EQ);
                    qbp1.setValue(querier_type);
                    qbps1.add(qbp1);
                    qc1.setBooleanPredicates(qbps1);
                    querierConditions.add(qc1);
                    QuerierCondition qc2 = new QuerierCondition();
                    qc2.setPolicy_id(id);
                    qc2.setAttribute("querier");
                    qc2.setType(AttributeType.STRING);
                    List<BooleanPredicate> qbps2 = new ArrayList<>();
                    BooleanPredicate qbp2 = new BooleanPredicate();
                    qbp2.setOperator(Operation.EQ);
                    qbp2.setValue(querier);
                    qbps2.add(qbp2);
                    qc2.setBooleanPredicates(qbps2);
                    querierConditions.add(qc2);
                    objectConditions = new ArrayList<>();
                }
                ObjectCondition oc = new ObjectCondition();
                oc.setAttribute(rs.getString(oc_table + ".attribute"));
                oc.setPolicy_id(rs.getString(oc_table + ".policy_id"));
                oc.setType(AttributeType.valueOf(rs.getString(oc_table + ".attribute_type")));
                List<BooleanPredicate> booleanPredicates = new ArrayList<>();
                BooleanPredicate bp1 = new BooleanPredicate();
                bp1.setOperator(convertOperator(rs.getString(oc_table + ".operator")));
                bp1.setValue(rs.getString(oc_table + ".comp_value"));
                rs.next();
                BooleanPredicate bp2 = new BooleanPredicate();
                bp2.setOperator(convertOperator(rs.getString(oc_table + ".operator")));
                bp2.setValue(rs.getString(oc_table + ".comp_value"));
                booleanPredicates.add(bp1);
                booleanPredicates.add(bp2);
                oc.setBooleanPredicates(booleanPredicates);
                objectConditions.add(oc);

                if (!rs.next()) {
                    BEPolicy bePolicy = new BEPolicy(id, objectConditions, querierConditions, purpose, action, inserted_at);
                    bePolicies.add(bePolicy);
                    break;
                }

                next = rs.getString(policy_table + ".id");
                if (!id.equalsIgnoreCase(next)) {
                    BEPolicy bePolicy = new BEPolicy(id, objectConditions, querierConditions, purpose, action, inserted_at);
                    bePolicies.add(bePolicy);
                    skip = false;
                }
                else skip = true;
            }
        }catch (SQLException e) {
            e.printStackTrace();
        }
        return bePolicies;
    }

    //TODO: fix reading object conditions like above
    public BEPolicy retrievePolicy(String querier, String querier_type, String inserted_after) {
        BEPolicy bePolicy = new BEPolicy();
        String id = null, purpose = null, action = null;
        Timestamp inserted_at = null;
        List<ObjectCondition> objectConditions = new ArrayList<>();

        List<QuerierCondition> querierConditions = new ArrayList<>();
        QuerierCondition qc1 = new QuerierCondition();
        qc1.setAttribute("policy_type");
        qc1.setType(AttributeType.STRING);
        List<BooleanPredicate> qbps1 = new ArrayList<>();
        BooleanPredicate qbp1 = new BooleanPredicate();
        qbp1.setOperator(Operation.EQ);
        qbp1.setValue(querier_type);
        qbps1.add(qbp1);
        qc1.setBooleanPredicates(qbps1);
        querierConditions.add(qc1);

        QuerierCondition qc2 = new QuerierCondition();
        qc2.setAttribute("querier");
        qc2.setType(AttributeType.STRING);
        List<BooleanPredicate> qbps2 = new ArrayList<>();
        BooleanPredicate qbp2 = new BooleanPredicate();
        qbp2.setOperator(Operation.EQ);
        qbp2.setValue(querier);
        qbps2.add(qbp2);
        qc2.setBooleanPredicates(qbps2);
        querierConditions.add(qc2);

        String policy_table = null, oc_table = null;
        if (querier_type.equalsIgnoreCase("user")) {
            policy_table = "USER_POLICY";
            oc_table = "USER_POLICY_OBJECT_CONDITION";
        } else if (querier_type.equalsIgnoreCase("group")) {
            policy_table = "GROUP_POLICY";
            oc_table = "GROUP_POLICY_OBJECT_CONDITION";
        }

        PreparedStatement queryStm = null;
        try {
            if(inserted_after == null) {
                queryStm = connection.prepareStatement("SELECT " + policy_table  + ".id, " + policy_table +".querier, " + policy_table +".purpose, " +
                        policy_table + ".enforcement_action," + policy_table +".inserted_at," + oc_table +".id, " + oc_table +" .policy_id," + oc_table + ".attribute, " +
                        oc_table + ".attribute_type, " + oc_table +".operator," + oc_table +".comp_value " +
                        "FROM "  + policy_table +", " + oc_table +
                        " WHERE " + policy_table + ".querier=? AND "+ policy_table + ".id = " + oc_table + ".policy_id");
                queryStm.setInt(1, Integer.parseInt(querier));

            }
            else{
                queryStm = connection.prepareStatement("SELECT " + policy_table  + ".id, " + policy_table +".querier, " + policy_table +".purpose, " +
                        policy_table + ".enforcement_action," + policy_table +".inserted_at," + oc_table +".id, " + oc_table +" .policy_id," + oc_table + ".attribute, " +
                        oc_table + ".attribute_type, " + oc_table +".operator," + oc_table +".comp_value " +
                        "FROM "  + policy_table +", " + oc_table +
                        " WHERE " + policy_table + ".querier=? AND "+ policy_table + ".id = " + oc_table + ".policy_id AND " +
                        policy_table +".inserted_at >=?");
                queryStm.setInt(1, Integer.parseInt(querier));

                try {
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Date parsedDate = dateFormat.parse(inserted_after);
                    Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());
                    queryStm.setTimestamp(2, timestamp);
                } catch(Exception e) {

                }
            }

            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) {
                if (id == null) {
                    id = rs.getString(policy_table +".id");
                    purpose = rs.getString(policy_table + ".purpose");
                    action = rs.getString(policy_table + ".enforcement_action");
                    inserted_at = rs.getTimestamp(policy_table +".inserted_at");
                }
                ObjectCondition oc = new ObjectCondition();
                oc.setAttribute(rs.getString(oc_table + ".attribute"));
                oc.setPolicy_id(rs.getString(oc_table + ".policy_id"));
                oc.setType(AttributeType.valueOf(rs.getString(oc_table + ".attribute_type")));
                List<BooleanPredicate> booleanPredicates = new ArrayList<>();
                BooleanPredicate bp1 = new BooleanPredicate();
                bp1.setOperator(Operation.valueOf(rs.getString(oc_table + ".operator")));
                bp1.setValue(rs.getString(oc_table +".comp_value"));
                rs.next();
                BooleanPredicate bp2 = new BooleanPredicate();
                bp2.setOperator(Operation.valueOf(rs.getString(oc_table +".operator")));
                bp2.setValue(rs.getString(oc_table +".comp_value"));
                booleanPredicates.add(bp1);
                booleanPredicates.add(bp2);
                oc.setBooleanPredicates(booleanPredicates);
                objectConditions.add(oc);

            }
            bePolicy = new BEPolicy(id, objectConditions, querierConditions, purpose, action, inserted_at);
        } catch (SQLException e) {
            e.printStackTrace();
        }
            return bePolicy;
    }

}
