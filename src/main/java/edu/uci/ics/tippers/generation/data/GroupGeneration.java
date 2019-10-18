package edu.uci.ics.tippers.generation.data;

import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.generation.policy.PolicyGen;
import edu.uci.ics.tippers.model.data.Presence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Generates groups of users based on their affinity to locations based on the data in Presence table
 *
 * Hashmap of <User, Affinity>
 *
 * 1. Read all locations and create user groups corresponding to locations (Write these to database)
 * 2. Read all the tuples from Presence table into a cursor
 * 3. For each tuple, check user and location and create a <User, Affinity> if it doesn't exist
 * 4. Add the time spent by the user in the location to that entry (TODO: can be improved with time spent each day)
 * 5.
 *
 */

public class GroupGeneration {
    private HashMap<Integer, List<Affinity>> colocations;
    private Connection connection;
    private PolicyGen pg;

    private final long GM_TIME_THRESHOLD = 1 * 24 * 60 * 1000; //1 day
    private final int GM_VISITS_THRESHOLD = (int) (30 * 3 * 0.05); //5% of the total days in 3 months

    private final double GRAD_ROLE = 90 * 0.7 * 8 * 60 * 60 * 1000; //1814400
    private final double FAC_ROLE = 90 * 0.6 * 8 * 60 * 60 * 1000; //1555200
    private final double STAFF_ROLE = 90 * 0.6 * 6 * 60 * 60 * 1000; //1166400
    private final double UGRAD_ROLE = 90 * 0.5 * 4 * 60 * 60 * 1000; //648000
    private final double JAN_ROLE = 90 * 0.8 * 1 * 60 * 60 * 1000; //259200 - 3 days in total

    public GroupGeneration(){
        this.colocations = new HashMap<>();
        this.connection = MySQLConnectionManager.getInstance().getConnection();
        this.pg = new PolicyGen();
    }

    /**
     * Groups are created per location entry and name of the group is the same as the room name
     * Inserted in USER_GROUP table
     */
    private void generateRoomGroups(){
        List<String> location_ids = pg.getAllLocations();
        String ugInsert = "INSERT INTO USER_GROUP (ID) VALUES (?)";
        try {
            PreparedStatement ugStmt = connection.prepareStatement(ugInsert);
            for (String lid: location_ids) {
                ugStmt.setString(1, lid);
                ugStmt.addBatch();
            }
            ugStmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Insert various roles into USER_GROUP_TABLE
     */
    private void generateRoleGroups(){
        String ugInsert = "INSERT INTO USER_GROUP (ID) VALUES (?)";
        try {
            PreparedStatement ugStmt = connection.prepareStatement(ugInsert);
            ugStmt.setString(1, "graduate");
            ugStmt.addBatch();
            ugStmt.setString(1, "undergrad");
            ugStmt.addBatch();
            ugStmt.setString(1, "faculty");
            ugStmt.addBatch();
            ugStmt.setString(1, "staff");
            ugStmt.addBatch();
            ugStmt.setString(1, "janitor");
            ugStmt.addBatch();
            ugStmt.setString(1, "visitor");
            ugStmt.addBatch();
            ugStmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    private void addLocTime(List<Affinity> affs, String location_id, long timeSpent) {
        for(Affinity af: affs){
            if (af!= null && af.getLocationId().equalsIgnoreCase(location_id)) {
                af.setTotalTime(af.getTotalTime().plusMillis(timeSpent));
                af.setNumOfTimes(af.getNumOfTimes()+1);
                return;
            }
        }
        Affinity uAff = new Affinity(location_id, timeSpent);
        affs.add(uAff);
    }

    private void generateAffinities(){
        PreparedStatement queryStm = null;
        try {
            queryStm = connection.prepareStatement("SELECT USER_ID as uid, LOCATION_ID as lid,  start, finish " +
                    "FROM PRESENCE limit 10000");
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) {
                Presence pt = new Presence();
                pt.setUser_id(rs.getInt("uid"));
                pt.setLocation_id(rs.getString("lid"));
                pt.setStart(rs.getTimestamp("start"));
                pt.setFinish(rs.getTimestamp("finish"));
                if(colocations.containsKey(pt.getUser_id())){
                    addLocTime(colocations.get(pt.getUser_id()), pt.getLocation_id(),
                            pt.getFinish().getTime() - pt.getStart().getTime());
                }
                else{
                    Affinity uAff = new Affinity(pt.getLocation_id(),
                            pt.getFinish().getTime() - pt.getStart().getTime());
                    List<Affinity> uAffList = new ArrayList<>();
                    uAffList.add(uAff);
                    colocations.put(pt.getUser_id(), uAffList);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    //TODO: different thresholds for different locations
    private boolean groupCheck(String lid, Duration totalTime, int numOfTimes){
        return (totalTime.toMillis() > GM_TIME_THRESHOLD && numOfTimes > GM_VISITS_THRESHOLD);
    }


    private String roleCheck(Duration inBuilding) {
        if(inBuilding.toMillis() > GRAD_ROLE) {
            return "graduate";
        } else if (inBuilding.toMillis() > FAC_ROLE) {
            return  "faculty";
        } else if (inBuilding.toMillis() > STAFF_ROLE) {
            return "staff";
        } else if (inBuilding.toMillis() > UGRAD_ROLE) {
            return "undergrad";
        } else if (inBuilding.toMillis() > JAN_ROLE){
            return "janitor";
        }
        return "visitor";
    }


    public void generateGroupMemberships(){
        String ugmInsert = "INSERT INTO USER_GROUP_MEMBERSHIP (USER_ID, USER_GROUP_ID) VALUES (?, ?)";
        try {
            PreparedStatement ugmStmt = connection.prepareStatement(ugmInsert);
            for (Map.Entry<Integer, List<Affinity>> entry : colocations.entrySet()) {
                int uid = entry.getKey();
                Duration inBuilding = Duration.ofMillis(0);
                List<Affinity> value = entry.getValue();
                for (Affinity aff: value) {
                    String lid = aff.getLocationId();
                    inBuilding = inBuilding.plus(aff.getTotalTime());
                    if(groupCheck(lid, aff.getTotalTime(), aff.getNumOfTimes())){
                        ugmStmt.setInt(1, uid);
                        ugmStmt.setString(2, lid);
                        ugmStmt.addBatch();
                    }
                }
                String urole = roleCheck(inBuilding);
                ugmStmt.setInt(1, uid);
                ugmStmt.setString(2, urole);
                ugmStmt.addBatch();
                ugmStmt.executeBatch();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    class Affinity {

        String locationId;
        Duration totalTime;
        int numOfTimes;

        public Affinity(String locationId, long totalTime) {
            this.locationId = locationId;
            this.totalTime = Duration.ofMillis(totalTime);
            this.numOfTimes = 1;
        }

        public String getLocationId() {
            return locationId;
        }

        public void setLocationId(String locationId) {
            this.locationId = locationId;
        }

        public Duration getTotalTime() {
            return totalTime;
        }

        public void setTotalTime(Duration totalTime) {
            this.totalTime = totalTime;
        }

        public int getNumOfTimes() {
            return numOfTimes;
        }

        public void setNumOfTimes(int numOfTimes) {
            this.numOfTimes = numOfTimes;
        }
    }

    public static void main(String [] args){
        GroupGeneration gg = new GroupGeneration();
//        gg.generateRoleGroups();
//        gg.generateRoomGroups();
//        gg.generateAffinities();
//        gg.generateGroupMemberships();
    }

}
