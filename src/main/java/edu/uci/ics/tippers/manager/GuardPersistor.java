package edu.uci.ics.tippers.manager;

import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.model.guard.GuardPart;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class GuardPersistor {

    private static GuardPersistor _instance = new GuardPersistor();

    private static Connection connection = MySQLConnectionManager.getInstance().getConnection();

    public static GuardPersistor getInstance() {
        return _instance;
    }


    public void insertGuard(GuardExp guardExp) {

        String guardExpTable, guardPartTable;
        if (guardExp.isUserGuard()) { //User Guard
            guardExpTable = "USER_GUARD";
            guardPartTable = "USER_GUARD_EXPRESSION";
        } else {
            guardExpTable = "GROUP_GUARD";
            guardPartTable = "GROUP_GUARD_EXPRESSION";
        }

        String userGuardInsert = "INSERT INTO " + guardExpTable +
                " (id, querier, purpose, enforcement_action, last_updated, dirty) VALUES (?, ?, ?, ?, ?, ?)";

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
                    " (guard_exp_id, guard, remainder) VALUES (?, ?, ?)";

            PreparedStatement gpStmt = connection.prepareStatement(guardExpInsert);
            for (GuardPart gp : guardExp.getGuardParts()) {
                gpStmt.setString(1, guardExp.getId());
                gpStmt.setString(2, gp.getGuardFactor());
                gpStmt.setString(3, gp.getGuardPartition());
                gpStmt.addBatch();
            }
            gpStmt.executeBatch();
        } catch (SQLException e) {
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
                gp.setId(rs.getString(guardPartTable+".id"));
                gp.setGuardFactor(rs.getString(guardPartTable+".guard"));
                gp.setGuardPartition(rs.getString(guardPartTable+".remainder"));
                guardParts.add(gp);
            }
            guardExp = new GuardExp(id, purpose, action, last_updated, guardParts);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return guardExp;
    }
}
