package edu.uci.ics.tippers.manager;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.model.guard.GuardPart;
import edu.uci.ics.tippers.model.policy.BEPolicy;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class GuardPersistor {

    private static GuardPersistor _instance = new GuardPersistor();

    private static Connection connection = MySQLConnectionManager.getInstance().getConnection();

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
                    " (guard_exp_id, ownerEq, profEq, groupEq, locEq, dateGe, dateLe, timeGe, timeLe, id) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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
                String gpID =  UUID.randomUUID().toString();
                gpStmt.setString(10, gpID);
                gpStmt.addBatch();
                for (BEPolicy bp: gp.getGuardPartition().getPolicies()) {
                    gpolStmt.setString(1, gpID);
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

    public GuardExp retrieveGuard(String querier, String querier_type){
        GuardExp guardExp = new GuardExp();

        String guardExpTable, guardPartTable;
        if (querier_type.equalsIgnoreCase("user")) { //User Guard
            guardExpTable = "USER_GUARD";
            guardPartTable = "USER_GUARD_EXPRESSION";
        } else {
            guardExpTable = "GROUP_GUARD";
            guardPartTable = "GROUP_GUARD_EXPRESSION";
        }
        String id = null, purpose = null, action = null;
        Timestamp last_updated = null;
        List<GuardPart> guardParts = new ArrayList<>();
        PreparedStatement queryStm = null;

        try {
            queryStm = connection.prepareStatement("SELECT " + guardExpTable  + ".id, " + guardExpTable +".querier, "
                    + guardExpTable +".purpose, " + guardExpTable + ".enforcement_action," + guardExpTable +".last_updated,"
                    + guardPartTable +".id, " + guardPartTable +" .guard_exp_id,"
                    + guardPartTable + ".guard, " + guardPartTable + ".remainder "+
                    "FROM "  + guardExpTable +", " + guardPartTable +
                    " WHERE " + guardExpTable + ".querier=? AND "+ guardExpTable + ".id = " + guardPartTable + ".guard_exp_id");
            queryStm.setInt(1, Integer.parseInt(querier));
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) {
                if (id == null) {
                    id = rs.getString(guardExpTable +".id");
                    purpose = rs.getString(guardExpTable + ".purpose");
                    action = rs.getString(guardExpTable + ".enforcement_action");
                    last_updated = rs.getTimestamp(guardExpTable +".last_updated");
                }
                GuardPart gp = new GuardPart();
                //TODO: To be completed
                guardParts.add(gp);
            }
            guardExp = new GuardExp(id, purpose, action, last_updated, guardParts);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return guardExp;
    }
}
