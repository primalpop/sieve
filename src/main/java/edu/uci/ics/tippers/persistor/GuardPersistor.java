package edu.uci.ics.tippers.persistor;

import edu.uci.ics.tippers.common.AttributeType;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.dbms.mysql.MySQLConnectionManager;
import edu.uci.ics.tippers.dbms.QueryManager;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.model.guard.GuardPart;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;
import edu.uci.ics.tippers.model.policy.Operation;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class GuardPersistor {

    private static GuardPersistor _instance = new GuardPersistor();

    private static Connection connection = MySQLConnectionManager.getInstance().getConnection();

    private static QueryManager queryManager = new QueryManager();

    public static GuardPersistor getInstance() {
        return _instance;
    }

    public void insertGuard(GuardExp guardExp) {

        String guardExpTable, guardPartTable, guardToPolicyTable;
        if (guardExp.isUserGuard()) { //User Guard
            guardExpTable = "USER_GUARD_EXPRESSION";
            guardPartTable = "USER_GUARD_PARTS";
            guardToPolicyTable = "USER_GUARD_TO_POLICY";
        } else {
            guardExpTable = "GROUP_GUARD_EXPRESSION";
            guardPartTable = "GROUP_GUARD_PARTS";
            guardToPolicyTable = "GROUP_GUARD_TO_POLICY";
        }

        String userGuardInsert = "INSERT INTO " + guardExpTable +
                " (id, querier, purpose, enforcement_action, last_updated, dirty) " +
                "VALUES (?, ?, ?, ?, ?, ?)";

        PreparedStatement userGuardStmt = null;
        try {

            userGuardStmt = connection.prepareStatement(userGuardInsert);
            userGuardStmt.setString(1, guardExp.getId());
            userGuardStmt.setInt(2, Integer.parseInt(guardExp.getQuerier()));
            userGuardStmt.setString(3, guardExp.getPurpose());
            userGuardStmt.setString(4, guardExp.getAction());
            userGuardStmt.setTimestamp(5, guardExp.getLast_updated());
            userGuardStmt.setString(6, guardExp.getDirty());
            userGuardStmt.executeUpdate();
            userGuardStmt.close();


            String guardExpInsert = "INSERT INTO " + guardPartTable +
                    " (guard_exp_id, ownerEq, profEq, groupEq, locEq, dateGe, dateLe, timeGe, timeLe, id, cardinality) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            String guardToPolicyInsert = "INSERT INTO " + guardToPolicyTable +
                "(guard_id, policy_id) VALUES (?, ?)";

            PreparedStatement gpStmt = connection.prepareStatement(guardExpInsert);
            PreparedStatement gpolStmt = connection.prepareStatement(guardToPolicyInsert);
            for (GuardPart gp : guardExp.getGuardParts()) {
                int ownerEq = 0;
                String profEq = null, groupEq = null, locEq = null;
                Date dateGe = null, dateLe = null;
                Time timeGe = null, timeLe = null;
                gpStmt.setString(1, guardExp.getId());
                if(gp.getGuard().getAttribute().equalsIgnoreCase(PolicyConstants.USERID_ATTR)){
                    ownerEq = Integer.parseInt(gp.getGuard().getBooleanPredicates().get(0).getValue());
                } else if(gp.getGuard().getAttribute().equalsIgnoreCase(PolicyConstants.PROFILE_ATTR)){
                    profEq = gp.getGuard().getBooleanPredicates().get(0).getValue();
                } else if(gp.getGuard().getAttribute().equalsIgnoreCase(PolicyConstants.GROUP_ATTR)){
                    groupEq = gp.getGuard().getBooleanPredicates().get(0).getValue();
                } else if(gp.getGuard().getAttribute().equalsIgnoreCase(PolicyConstants.LOCATIONID_ATTR)){
                    locEq = gp.getGuard().getBooleanPredicates().get(0).getValue();
                } else if(gp.getGuard().getAttribute().equalsIgnoreCase(PolicyConstants.START_DATE)){
                    SimpleDateFormat sdf = new SimpleDateFormat(PolicyConstants.DATE_FORMAT);
                    dateGe = new java.sql.Date(sdf.parse(gp.getGuard().getBooleanPredicates().get(0).getValue()).getTime());
                    dateLe = new java.sql.Date(sdf.parse(gp.getGuard().getBooleanPredicates().get(1).getValue()).getTime());
                } else if(gp.getGuard().getAttribute().equalsIgnoreCase(PolicyConstants.START_TIME)){
                    SimpleDateFormat sdf = new SimpleDateFormat(PolicyConstants.TIME_FORMAT);
                    timeGe = new java.sql.Time(sdf.parse(gp.getGuard().getBooleanPredicates().get(0).getValue()).getTime());
                    timeLe = new java.sql.Time(sdf.parse(gp.getGuard().getBooleanPredicates().get(1).getValue()).getTime());
                }
                double gpSel = queryManager.checkSelectivity(gp.getGuard().print());
                if(ownerEq == 0)
                    gpStmt.setNull(2, Types.INTEGER);
                else
                    gpStmt.setInt(2, ownerEq);
                gpStmt.setString(3, profEq);
                gpStmt.setString(4, groupEq);
                gpStmt.setString(5, locEq);
                gpStmt.setDate(6, dateGe);
                gpStmt.setDate(7, dateLe);
                gpStmt.setTime(8, timeGe);
                gpStmt.setTime(9, timeLe);
                gpStmt.setString(10, gp.getId());
                gpStmt.setFloat(11, (float) gpSel);
                gpStmt.addBatch();
                for (BEPolicy bp: gp.getGuardPartition().getPolicies()) {
                    gpolStmt.setString(1, gp.getId());
                    gpolStmt.setString(2, bp.getId());
                    gpolStmt.addBatch();
                }
            }
            gpStmt.executeBatch();
            gpolStmt.executeBatch();

        } catch (SQLException | ParseException e) {
            e.printStackTrace();
        }
    }


    public BEExpression retrieveGuardPartition(String guard_id, String guard_to_policy_table, List<BEPolicy> allowPolicies){
        List<BEPolicy> guardPolicies = new ArrayList<>();
        PreparedStatement queryStm = null;
        List<String> policy_ids = new ArrayList<>();
        try{
            queryStm = connection.prepareStatement("SELECT " + guard_to_policy_table  + ".policy_id "
                    + "FROM "  + guard_to_policy_table +
                    " WHERE " + guard_to_policy_table + ".guard_id=? ");
            queryStm.setString(1, guard_id);
            ResultSet rs = queryStm.executeQuery();
            while(rs.next()){
                policy_ids.add(rs.getString(guard_to_policy_table  + ".policy_id"));
            }
        } catch(SQLException e) {
            e.printStackTrace();
        }
        for (String pid: policy_ids) {
            for(BEPolicy bePolicy: allowPolicies){
                if (bePolicy.getId().equalsIgnoreCase(pid)){
                    guardPolicies.add(bePolicy);
                }
            }
        }
        return new BEExpression(guardPolicies);
    }

    /**
     * Retrives the guard based on Querier and Querier type
     * TODO: retrieve the policies in the guard partition
     * @param querier
     * @param querier_type
     * @return
     */
    public GuardExp retrieveGuardExpression(String querier, String querier_type, List<BEPolicy> allowPolicies){
        String guardExpTable, guardPartTable, guardToPolicyTable;
        if (querier_type.equalsIgnoreCase("user")) { //User Guard
            guardExpTable = "USER_GUARD_EXPRESSION";
            guardPartTable = "USER_GUARD_PARTS";
            guardToPolicyTable = "USER_GUARD_TO_POLICY";
        } else {
            guardExpTable = "GROUP_GUARD_EXPRESSION";
            guardPartTable = "GROUP_GUARD_PARTS";
            guardToPolicyTable = "GROUP_GUARD_TO_POLICY";
        }
        String id = null, purpose = null, action = null;
        Timestamp last_updated = null;
        List<GuardPart> guardParts = new ArrayList<>();
        PreparedStatement queryStm = null;
        try {
            queryStm = connection.prepareStatement("SELECT " + guardExpTable  + ".id, " + guardExpTable +".querier, "
                    + guardExpTable +".purpose, " + guardExpTable + ".enforcement_action," + guardExpTable +".last_updated,"
                    + guardPartTable +".id, " + guardPartTable +" .guard_exp_id,"
                    + guardPartTable + ".ownerEq, " + guardPartTable + ".profEq, "
                    + guardPartTable + ".groupEq, " + guardPartTable + ".locEq, "
                    + guardPartTable + ".dateGe, " + guardPartTable + ".dateLe, "
                    + guardPartTable + ".timeGe, " + guardPartTable + ".timeLe, "
                    + guardPartTable + ".cardinality "
                    + "FROM "  + guardExpTable +", " + guardPartTable +
                    " WHERE " + guardExpTable + ".querier=? AND "+ guardExpTable + ".id = " + guardPartTable + ".guard_exp_id"
                    + " order by " + guardExpTable  + ".id");
            queryStm.setInt(1, Integer.parseInt(querier));
            ResultSet rs = queryStm.executeQuery();
            List<GuardPart> gps = new ArrayList<>();
            boolean skip = false;
            while (rs.next()) {
                if (!skip) {
                    id = rs.getString(guardExpTable +".id");
                    purpose = rs.getString(guardExpTable + ".purpose");
                    action = rs.getString(guardExpTable + ".enforcement_action");
                    last_updated = rs.getTimestamp(guardExpTable +".last_updated");
                    skip = true;
                }
                GuardPart gp = new GuardPart();
                gp.setId(rs.getString(guardPartTable + ".id"));
                if(rs.getString(guardPartTable+ ".ownerEq") != null){
                    ObjectCondition objectCondition = new ObjectCondition(null, PolicyConstants.USERID_ATTR,
                            AttributeType.STRING, rs.getString(guardPartTable+ ".ownerEq"), Operation.EQ,
                            rs.getString(guardPartTable+ ".ownerEq"), Operation.EQ);
                    gp.setGuard(objectCondition);
                }
                else if(rs.getString(guardPartTable+ ".profEq") != null){
                    ObjectCondition objectCondition = new ObjectCondition(null, PolicyConstants.PROFILE_ATTR,
                            AttributeType.STRING, rs.getString(guardPartTable+ ".profEq"), Operation.EQ,
                            rs.getString(guardPartTable+ ".profEq"), Operation.EQ);
                    gp.setGuard(objectCondition);
                }
                else if(rs.getString(guardPartTable+ ".groupEq") != null){
                    ObjectCondition objectCondition = new ObjectCondition(null, PolicyConstants.GROUP_ATTR,
                            AttributeType.STRING, rs.getString(guardPartTable+ ".groupEq"), Operation.EQ,
                            rs.getString(guardPartTable+ ".groupEq"), Operation.EQ);
                    gp.setGuard(objectCondition);
                }
                else if(rs.getString(guardPartTable+ ".locEq") != null){
                    ObjectCondition objectCondition = new ObjectCondition(null, PolicyConstants.LOCATIONID_ATTR,
                            AttributeType.STRING, rs.getString(guardPartTable+ ".locEq"), Operation.EQ,
                            rs.getString(guardPartTable+ ".locEq"), Operation.EQ);
                    gp.setGuard(objectCondition);
                }
                else if((rs.getString(guardPartTable+ ".dateGe") != null) &&
                        (rs.getString(guardPartTable+ ".dateLe") != null)){
                    ObjectCondition objectCondition = new ObjectCondition(null, PolicyConstants.START_DATE,
                            AttributeType.DATE, rs.getString(guardPartTable+ ".dateGe"), Operation.GTE,
                            rs.getString(guardPartTable+ ".dateLe"), Operation.LTE);
                    gp.setGuard(objectCondition);
                }
                else {
                    ObjectCondition objectCondition = new ObjectCondition(null, PolicyConstants.START_TIME,
                            AttributeType.TIME, rs.getString(guardPartTable+ ".timeGe"), Operation.GTE,
                            rs.getString(guardPartTable+ ".timeLe"), Operation.LTE);
                    gp.setGuard(objectCondition);
                }
                gp.setCardinality(rs.getFloat(guardPartTable + ".cardinality"));
                gp.setGuardPartition(retrieveGuardPartition(gp.getId(), guardToPolicyTable, allowPolicies));
                guardParts.add(gp);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new GuardExp(id, purpose, action, last_updated, guardParts);
    }
}
