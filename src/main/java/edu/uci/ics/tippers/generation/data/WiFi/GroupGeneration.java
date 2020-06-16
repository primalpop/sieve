package edu.uci.ics.tippers.generation.data.WiFi;

import edu.uci.ics.tippers.dbms.mysql.MySQLConnectionManager;
import edu.uci.ics.tippers.generation.policy.WiFiDataSet.PolicyUtil;
import edu.uci.ics.tippers.model.data.*;

import java.sql.*;
import java.time.Duration;
import java.util.*;

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
    private static final double MULTIPLE_GROUP_FACTOR = 0.5;
    private static final int MAX_GROUP_MEMBERSHIP = 1;
    private HashMap<User, List<Affinity>> colocations;
    private Connection connection;
    private PolicyUtil pg;

    private final double GRADUATE_TIME = 90 * 0.7 * 8 * 60 * 60 * 1000; //1814400
    private final double FACULTY_TIME = 90 * 0.6 * 8 * 60 * 60 * 1000; //1555200
    private final double STAFF_TIME = 90 * 0.6 * 6 * 60 * 60 * 1000; //1166400
    private final double UNDERGRAD_TIME = 90 * 0.5 * 4 * 60 * 60 * 1000; //648000

    public GroupGeneration(){
        this.colocations = new HashMap<User, List<Affinity>>();
        this.connection = MySQLConnectionManager.getInstance().getConnection();
        this.pg = new PolicyUtil();
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
            for (int i = 0; i < location_ids.size(); i++) {
                String lid = location_ids.get(i);
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
            ugStmt.setString(1, UserProfile.GRADUATE.getValue());
            ugStmt.addBatch();
            ugStmt.setString(1, UserProfile.UNDERGRAD.getValue());
            ugStmt.addBatch();
            ugStmt.setString(1, UserProfile.FACULTY.getValue());
            ugStmt.addBatch();
            ugStmt.setString(1, UserProfile.STAFF.getValue());
            ugStmt.addBatch();
            ugStmt.setString(1, UserProfile.VISITOR.getValue());
            ugStmt.addBatch();
            ugStmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    private void addLocTime(List<Affinity> affs, Location location, long timeSpent) {
        for(Affinity af: affs){
            if (af!= null && af.getLocation().equals(location)) {
                af.setTotalTime(af.getTotalTime().plusMillis(timeSpent));
                af.setNumOfTimes(af.getNumOfTimes()+1);
                return;
            }
        }
        Affinity uAff = new Affinity(location, timeSpent);
        affs.add(uAff);
    }

    private void generateAffinities(){
        PreparedStatement queryStm = null;
        try {
            queryStm = connection.prepareStatement("SELECT USER_ID as uid, LOCATION_ID as lid,  start, finish " +
                    "FROM PRESENCE"); //TODO: limit only for testing
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) {
                User user = new User(rs.getInt("uid"));
                Location loc = new Location(rs.getString("lid"));
                Timestamp start = rs.getTimestamp("start");
                Timestamp finish = rs.getTimestamp("finish");
                if(colocations.containsKey(user)){
                    addLocTime(colocations.get(user), loc,
                            finish.getTime() - start.getTime());
                }
                else{
                    Affinity uAff = new Affinity(loc,
                            finish.getTime() - start.getTime());
                    List<Affinity> uAffList = new ArrayList<>();
                    uAffList.add(uAff);
                    colocations.put(user, uAffList);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    private List<User> addToGroups(){
        List<User> users = new ArrayList<>();
        for (Map.Entry<User, List<Affinity>> entry : colocations.entrySet()) {
            User user = entry.getKey();
            Duration inBuilding = Duration.ofMillis(0);
            List<Affinity> value = entry.getValue();
            for (Affinity aff: value) {
                inBuilding = inBuilding.plus(aff.getTotalTime());
            }
            user.setProfile(roleCheck(inBuilding));
            value.sort(Collections.reverseOrder());
            if(user.getProfile() != UserProfile.VISITOR){
                List<UserGroup> ugs = new ArrayList<>();
                UserGroup ug = new UserGroup(value.get(0).getLocation().getName());
                ugs.add(ug);
                int numOfGroups = 1;
                for (int i = 1; i <value.size(); i++) {
                    if (value.get(i).getTotalTime().toMillis() > MULTIPLE_GROUP_FACTOR *
                            value.get(0).getTotalTime().toMillis()){
                        if (numOfGroups < MAX_GROUP_MEMBERSHIP) {
                            ugs.add(new UserGroup(value.get(i).getLocation().getName()));
                            numOfGroups += 1;
                        }
                    }
                }
                user.setGroups(ugs);
            }
            users.add(user);
        }
        return users;
    }

    private UserProfile roleCheck(Duration inBuilding) {
        if(inBuilding.toMillis() > GRADUATE_TIME) {
            return UserProfile.GRADUATE;
        } else if (inBuilding.toMillis() > FACULTY_TIME) {
            return  UserProfile.FACULTY;
        } else if (inBuilding.toMillis() > STAFF_TIME) {
            return UserProfile.STAFF;
        } else if (inBuilding.toMillis() > UNDERGRAD_TIME) {
            return UserProfile.UNDERGRAD;
        }
        return UserProfile.VISITOR;
    }


    public void generateGroupMemberships(){
        String ugmInsert = "INSERT INTO USER_GROUP_MEMBERSHIP (USER_ID, USER_GROUP_ID) VALUES (?, ?)";
        try {
            PreparedStatement ugmStmt = connection.prepareStatement(ugmInsert);
            List<User> userInGroups = addToGroups();
            for (User user: userInGroups) {
                if(user.getProfile() != UserProfile.VISITOR) {
                    for (UserGroup ug: user.getGroups()) {
                        ugmStmt.setInt(1, user.getUserId());
                        ugmStmt.setString(2, ug.getName());
                        ugmStmt.addBatch();
                    }
                }
                ugmStmt.setInt(1, user.getUserId());
                ugmStmt.setString(2, user.getProfile().getValue());
                ugmStmt.addBatch();
                ugmStmt.executeBatch();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    class Affinity implements Comparable<Affinity> {

        Location location;
        Duration totalTime;
        int numOfTimes;

        public Affinity(Location location, long totalTime) {
            this.location = location;
            this.setTotalTime(Duration.ofMillis(totalTime));
            this.numOfTimes = 1;
        }

        public Location getLocation() {
            return location;
        }

        public void setLocation(Location location) {
            this.location = location;
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


        @Override
        public int compareTo(Affinity o) {
            if(this.getTotalTime() == o.getTotalTime())
                return 0;
            else return this.getTotalTime().compareTo(o.getTotalTime());
        }
    }

    public static void main(String [] args){
        GroupGeneration gg = new GroupGeneration();
//        gg.generateRoleGroups();
//        gg.generateRoomGroups();
        gg.generateAffinities();
        gg.generateGroupMemberships();
    }

}
