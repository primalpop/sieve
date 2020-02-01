package edu.uci.ics.tippers.generation.query;

import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.generation.policy.PolicyGen;
import edu.uci.ics.tippers.model.policy.TimeStampPredicate;
import edu.uci.ics.tippers.model.query.QueryStatement;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class QueryGeneration {

    private List<Integer> user_ids;
    private List<String> locations;
    private List<String> user_groups;
    Timestamp start_beg, start_fin;
    private MySQLQueryManager mySQLQueryManager;
    private Connection connection = MySQLConnectionManager.getInstance().getConnection();
    private List<Integer> hours;
    private List<Integer> numUsers;
    private List<Integer> numLocs;
    double lowSelDown, lowSelUp;
    double medSelDown, medSelUp;
    double highSelDown, highSelUp;

    private PolicyGen pg; //helper methods defined in this class

    public QueryGeneration() {
        pg = new PolicyGen();
        this.user_ids = pg.getAllUsers(false);
        this.locations = pg.getAllLocations();
        this.start_beg = pg.getDate("MIN");
        this.start_fin = pg.getDate("MAX");
        this.user_groups = new ArrayList<>();
        user_groups.addAll(locations);
        user_groups.addAll(new ArrayList<>(Arrays.asList("faculty", "staff", "undergrad", "graduate", "visitor")));
        hours = new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5, 7, 10, 12, 15, 17, 20, 23));
//        numUsers = new ArrayList<Integer>(Arrays.asList(10, 20, 30, 40, 50, 75, 100, 150, 200, 250, 300, 350, 400, 450,
//                500, 550, 600, 650, 700, 750, 800, 850, 1000, 1250, 1500, 2000));
        numUsers = new ArrayList<Integer>(Arrays.asList(1000, 2000, 3000, 5000, 10000, 11000, 12000, 13000, 14000, 15000));
        numLocs = new ArrayList<Integer>(Arrays.asList(1, 2, 3, 5, 10, 15, 20, 30, 50));

        lowSelDown = 0.00001;
        lowSelUp = 0.001;
        medSelDown = 0.001;
        medSelUp = 0.2;
        highSelDown = 0.3;
        highSelUp = 0.5;

        this.mySQLQueryManager = new MySQLQueryManager();
        Random r = new Random();
    }

    private double getSelectivity(String selType){
        if(selType.equalsIgnoreCase("low")){
            return lowSelDown + Math.random() * (lowSelUp - lowSelDown);
        }
        else if(selType.equalsIgnoreCase("medium")){
            return medSelDown + Math.random() * (medSelUp - medSelDown);
        }
        else
            return highSelDown + Math.random() * (highSelUp - highSelDown);
    }

    private String checkStaticRangeSelectivity(double selectivity) {
        String selType = null;
        if (lowSelDown < selectivity && selectivity < lowSelUp) selType = "low";
        if (medSelDown < selectivity && selectivity < medSelUp) selType = "medium";
        if (highSelDown < selectivity && selectivity < highSelUp) selType = "high";
        return selType;
    }

    private String checkSelectivityType(double chosenSel, double selectivity) {
        String selType = null;
        if (chosenSel/10 < selectivity && selectivity < chosenSel) selType = "low";
        if (chosenSel/5 < selectivity && selectivity < chosenSel * 5) selType = "medium";
        if (chosenSel/2 < selectivity) selType = "high";
        return selType;
    }


    /**
     * Query 1: Select * from PRESENCE as P1 where P1.location_id in [......]
     * and start_date >= d1 and start_date <= d2 and
     * start_time >= t1 and start_time <= t2
     * The algorithm iterates from low to high selectivity types and
     * progressively increases the number of location predicates and/or time range
     * to satisfy the required selectivity except when the query is jumping ahead
     * in selectivity (e.g., chosen selType = low, generated query = medium) in which
     * case it waits instead.
     * @param selTypes   - types of query selectivity needed
     * @param queryCount - number of each queries of each selectivity type
     * @return
     */
    private List<QueryStatement> createQuery1(List<String> selTypes, int queryCount) {
        List<QueryStatement> queries = new ArrayList<>();
        int i = 0, j = 0;
        for (int k = 0; k < selTypes.size(); k++) {
            int numQ = 0;
            int locs = numLocs.get(i);
            int duration = 30; // in minutes
            int days = 0;
            boolean locFlag = true;
            String selType = selTypes.get(k);
            double chosenSel = getSelectivity(selType);
            System.out.println("Chosen Selectivity " + chosenSel);
            do {
                duration = Math.min(duration, 1439); //maximum of a day
                TimeStampPredicate tsPred = new TimeStampPredicate(pg.getDate("MIN"), days, "00:00", duration);
                String query = String.format("start_date >= \"%s\" AND start_date <= \"%s\" ", tsPred.getStartDate().toString(),
                        tsPred.getEndDate().toString());
                query += String.format("and start_time >= \"%s\" AND start_time <= \"%s\" ", tsPred.getStartTime().toString(),
                        tsPred.getEndTime().toString());
                List<String> locPreds = new ArrayList<>();
                int predCount = 0;
                while (predCount < locs) {
                    locPreds.add(String.valueOf(locations.get(new Random().nextInt(locations.size()))));
                    predCount += 1;
                }
                query += "AND location_id in (";
                query += locPreds.stream().map(item -> "\"" + item + "\", ").collect(Collectors.joining(" "));
                query = query.substring(0, query.length() - 2); //removing the extra comma
                query += ")";
                float selQuery = mySQLQueryManager.checkSelectivity(query);
                String querySelType = checkSelectivityType(chosenSel, selQuery);
                if(querySelType == null){
                    if(selType.equalsIgnoreCase("low") || selType.equalsIgnoreCase("medium")) {
                        if (duration < 1439) duration += 30;
                        else if (locFlag && ++i < numLocs.size()) {
                            locs = numLocs.get(i);
                            locFlag = false;
                        } else if (!locFlag && days < 90) {
                            days += 1;
                            locFlag = true;
                        }
                    }
                    else{
                        if (duration < 1439) duration += 200;
                        else if (locFlag && ++i < numLocs.size()) {
                            locs = numLocs.get(i);
                            locFlag = false;
                        } else if (!locFlag && days < 90) {
                            days += 5;
                            if(days % 10 == 0) locFlag = true;
                        }
                    }
                    continue;
                }
                if(selType.equalsIgnoreCase(querySelType)) {
                    System.out.println("Adding query with " + querySelType + " selectivity");
                    queries.add(new QueryStatement(query, 1, selQuery, querySelType,
                            new Timestamp(System.currentTimeMillis())));
                    numQ++;
                    chosenSel = getSelectivity(selType);
                    System.out.println("Chosen Selectivity " + chosenSel);
                    continue;
                }
                if (selType.equalsIgnoreCase("low")){
                    if (querySelType.equalsIgnoreCase("medium")
                            || querySelType.equalsIgnoreCase("high")){
                        duration = duration - 15;
                    }
                }
                else if (selType.equalsIgnoreCase("medium")) {
                    if(querySelType.equalsIgnoreCase("low")) {
                        if (duration < 1439) duration += 200;
                        else if (locFlag && ++i < numLocs.size()) {
                            locs = numLocs.get(i);
                            locFlag = false;
                        } else if (!locFlag && days < 90) {
                            days += 1;
                            locFlag = true;
                        }
                    }
                    else if(querySelType.equalsIgnoreCase("high")){
                        duration = duration - 15;
                    }
                }
                else {
                    if (querySelType.equalsIgnoreCase("low")
                            || querySelType.equalsIgnoreCase("medium")){
                        if (duration < 1439) duration += 200;
                        else if (locFlag && ++i < numLocs.size()) {
                            locs = numLocs.get(i);
                            locFlag = false;
                        } else if (!locFlag && days < 90) {
                            days += 2;
                            if(days % 5 == 0) locFlag = true;
                        }
                    }
                }
            } while (numQ < queryCount);
        }
        return queries;
    }

    /**
     * Query 2: Select * from PRESENCE P1 where P1.user_id in [......]
     * and start_date >= d1 and start_date <= d2 and
     * start_time >= t1 and start_time <= t2
     * The algorithm iterates from low to high selectivity types and
     * progressively increases the number of user predicates and/or time range
     * to satisfy the required selectivity except when the query is jumping ahead
     * in selectivity (e.g., chosen selType = low, generated query = medium) in which
     * case it waits instead.
     * @param selTypes   - types of query selectivity needed
     * @param queryCount - number of each queries of each selectivity type
     * @return
     */
    private List<QueryStatement> createQuery2(List<String> selTypes, int queryCount) {
        List<QueryStatement> queries = new ArrayList<>();
        int i = 0, j = 0;
        for (int k = 0; k < selTypes.size(); k++) {
            int numQ = 0;
            int userCount = numUsers.get(i);
            int duration = 300; // in minutes
            int days = 0;
            boolean userFlag = false;
            String selType = selTypes.get(k);
            double chosenSel = getSelectivity(selType);
            System.out.println("Chosen Selectivity " + chosenSel);
            do {
                duration = Math.min(duration, 1439); //maximum of a day
                TimeStampPredicate tsPred = new TimeStampPredicate(pg.getDate("MIN"), days, "00:00", duration);
                String query = String.format("start_date >= \"%s\" AND start_date <= \"%s\" ", tsPred.getStartDate().toString(),
                        tsPred.getEndDate().toString());
                query += String.format("and start_time >= \"%s\" AND start_time <= \"%s\" ", tsPred.getStartTime().toString(),
                        tsPred.getEndTime().toString());
                List<Integer> userPreds = new ArrayList<>();
                int predCount = 0;
                while (predCount < userCount) {
                    userPreds.add(user_ids.get(new Random().nextInt(user_ids.size())));
                    predCount += 1;
                }
                query += "AND user_id in (";
                query += userPreds.stream().map(item -> "\"" + item + "\", ").collect(Collectors.joining(" "));
                query = query.substring(0, query.length() - 2); //removing the extra comma
                query += ")";
                float selQuery = mySQLQueryManager.checkSelectivity(query);
                String querySelType = checkSelectivityType(chosenSel, selQuery);
                if(querySelType == null){
                    if(selType.equalsIgnoreCase("low") || selType.equalsIgnoreCase("medium")) {
                        if (duration < 1439) duration += 100;
                        else if (userFlag && ++i < numUsers.size()) {
                            userCount = numUsers.get(i);
                            userFlag = false;
                        } else if (!userFlag && days < 90) {
                            days += 1;
                            userFlag = true;
                        }
                    }
                    else {
                        if (duration < 1439) duration += 500;
                        else if (userFlag && ++i < numUsers.size()) {
                            userCount = numUsers.get(i);
                            userFlag = false;
                        } else if (!userFlag && days < 90) {
                            days += 10;
                            userFlag = true;
                        }
                        else
                            userFlag = true;
                    }
                    continue;
                }
                if(selType.equalsIgnoreCase(querySelType)) {
                    System.out.println("Adding query with " + querySelType + " selectivity");
                    queries.add(new QueryStatement(query, 2, selQuery, querySelType,
                            new Timestamp(System.currentTimeMillis())));
                    numQ++;
                    chosenSel = getSelectivity(selType);
                    System.out.println("Chosen Selectivity " + chosenSel);
                    continue;
                }
                if (selType.equalsIgnoreCase("low")){
                    if (querySelType.equalsIgnoreCase("medium")
                            || querySelType.equalsIgnoreCase("high")){
                        duration = duration - 200;
                        i--;
                    }
                }
                else if (selType.equalsIgnoreCase("medium")) {
                    if(querySelType.equalsIgnoreCase("low")) {
                        if (duration < 1439) duration += 200;
                        else if (userFlag && ++i < numUsers.size()) {
                            userCount = numUsers.get(i);
                            userFlag = false;
                        } else if (!userFlag && days < 90) {
                            days += 1;
                            if(days % 2 == 0) userFlag = true;
                        }
                    }
                    else if(querySelType.equalsIgnoreCase("high")){
                        duration = duration - 200;
                        days -= 2;
                        userCount -= 50;
                    }
                }
                else {
                    if (querySelType.equalsIgnoreCase("low")
                            || querySelType.equalsIgnoreCase("medium")){
                        if (duration < 1439) duration += 200;
                        else if (userFlag && ++i < numUsers.size()) {
                            userCount = numUsers.get(i);
                            userFlag = false;
                        } else if (!userFlag && days < 90) {
                            days += 1;
                            if(days % 2 == 0) userFlag = true;
                        }
                    }
                }
            } while (numQ < queryCount);
        }
        return queries;
    }

    /**
     * Query 3: Select user_id from PRESENCE as p, USER_GROUP_MEMBERSHIP as m
     * where m.id = "x" and p.user_id = m.user_id and
     * p.ts < time and ps.date ();
     * @return
     */
    private List<QueryStatement> createQuery3() {
        List<QueryStatement> queries = new ArrayList<>();
        for (int k = 0; k < user_groups.size(); k++) {
            for (int i = 5; i < 90 ; i=i+10) {
                String query = String.format("Select p.user_id from PRESENCE as p, USER_GROUP_MEMBERSHIP as m " +
                        "where m.user_group_id = \"%s\" AND p.user_id = m.user_id AND ", user_groups.get(k));
                TimeStampPredicate tsPred = new TimeStampPredicate(pg.getDate("MIN"), i, "00:00", 1439);
                query += String.format("start_date >= \"%s\" AND start_date <= \"%s\" ", tsPred.getStartDate().toString(),
                        tsPred.getEndDate().toString());
                float selQuery = mySQLQueryManager.checkSelectivityFullQuery(query);
                String querySelType = checkStaticRangeSelectivity(selQuery);
                if (querySelType == null) continue;
                queries.add(new QueryStatement(query, 3, selQuery, querySelType,
                        new Timestamp(System.currentTimeMillis())));
            }
        }
        return queries;

    }


    /**
     * Query 4: Select location_id, count(*) from PRESENCE
     * where ts-time < time and ts-date < date group by location_id;
     * @return
     */
    private List<QueryStatement> createQuery4() {
        List<QueryStatement> queries = new ArrayList<>();
        for (int j =0; j < 200; j++) {
            TimeStampPredicate tsPred = new TimeStampPredicate(pg.getDate("MIN"), 0, "00:00", 7*j);
            String query = String.format("Select location_id, count(*) from PRESENCE where start_time >= \"%s\" " +
                            "AND start_time <= \"%s\" group by location_id", tsPred.getStartTime().toString(),
                    tsPred.getEndTime().toString());
            String query_for_sel = String.format("start_time >= \"%s\" AND start_time <= \"%s\" ", tsPred.getStartTime().toString(),
                    tsPred.getEndTime().toString());
            float sel = mySQLQueryManager.checkSelectivity(query_for_sel);
            if (checkStaticRangeSelectivity(sel) == null) continue;
            queries.add(new QueryStatement(query, 4, sel,
                    checkStaticRangeSelectivity(sel), new Timestamp(System.currentTimeMillis())));
            System.out.println("Added query of selectivity " + checkStaticRangeSelectivity(sel));
        }
        int duration =  1439; //maximum of a day
//        for (int i = 0; i < 90; i=i+3) {
//            TimeStampPredicate tsPred = new TimeStampPredicate(pg.getDate("MIN"), i, "00:00", duration);
//            String query = String.format("Select location_id, count(*) from PRESENCE where start_date >= \"%s\" " +
//                            "AND start_date <= \"%s\" group by location_id", tsPred.getStartDate().toString(),
//                    tsPred.getEndDate().toString());
//            String query_for_sel = String.format("start_date >= \"%s\" AND start_date <= \"%s\" ", tsPred.getStartDate().toString(),
//                    tsPred.getEndDate().toString());
//            float sel = mySQLQueryManager.checkSelectivity(query_for_sel);
//            if (checkStaticRangeSelectivity(sel) == null) continue;
//            queries.add(new QueryStatement(query, 4, sel,
//                    checkStaticRangeSelectivity(sel), new Timestamp(System.currentTimeMillis())));
//            System.out.println("Added query of selectivity " + checkStaticRangeSelectivity(sel));
//        }
        return queries;
    }

    private List<QueryStatement> getQueries(int templateNum, List<String> selTypes, int numOfQueries) {
        if (templateNum == 0) {
            return createQuery1(selTypes, numOfQueries);
        } else if (templateNum == 1) {
            return createQuery2(selTypes, numOfQueries);
        } else if (templateNum == 2) {
            return createQuery3();
        } else if (templateNum == 3) {
            return createQuery4();
        }
        return null;
    }

    private void insertQuery(List<QueryStatement> queryStatements) {
        String soInsert = "INSERT INTO queries " +
                "(query_statement, template, selectivity, selectivity_type, inserted_at) " +
                "VALUES (?, ?, ?, ?, ?)";
        try {

            PreparedStatement stm = connection.prepareStatement(soInsert);
            int queryCount = 0;
            for (QueryStatement qs : queryStatements) {
                stm.setString(1, qs.getQuery());
                stm.setInt(2, qs.getTemplate());
                stm.setFloat(3, qs.getSelectivity());
                stm.setString(4, qs.getSelectivity_type());
                stm.setTimestamp(5, qs.getInserted_at());
                stm.addBatch();
                queryCount++;
                if (queryCount % 100 == 0) {
                    stm.executeBatch();
                    System.out.println("# " + queryCount + " inserted");
                }
            }
            stm.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public List<QueryStatement> retrieveQueries(int template, String selectivity_type, int query_count) {
        List<QueryStatement> queryStatements = new ArrayList<>();
        PreparedStatement queryStm = null;
        try {
            if (selectivity_type.equalsIgnoreCase("all")) {
                queryStm = connection.prepareStatement("SELECT id, query_statement, selectivity FROM queries as q " +
                        "WHERE q.template = ? order by selectivity limit " + query_count);
                queryStm.setInt(1, template);
            }
            else {
                queryStm = connection.prepareStatement("SELECT id, query_statement, selectivity FROM queries as q " +
                        "WHERE q.selectivity_type = ? AND q.template = ? order by selectivity limit " + query_count);
                queryStm.setString(1, selectivity_type);
                queryStm.setInt(2, template);
            }
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) {
                QueryStatement qs = new QueryStatement();
                qs.setQuery(rs.getString("query_statement"));
                qs.setId(rs.getInt("id"));
                qs.setSelectivity(rs.getFloat("selectivity"));
                qs.setTemplate(template);
                queryStatements.add(qs);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return queryStatements;
    }


    public void constructWorkload(boolean[] templates, int numOfQueries) {
        List<String> selTypes = new ArrayList<>();
        selTypes.add("low");
        selTypes.add("medium");
        selTypes.add("high");
        List<QueryStatement> queries = new ArrayList<>();
        for (int i = 0; i < templates.length; i++) {
            if (templates[i]) queries.addAll(getQueries(i, selTypes, numOfQueries));
        }
        insertQuery(queries);
    }

    public static void main(String[] args) {
        QueryGeneration qg = new QueryGeneration();
        boolean[] templates = {false, false, false, true};
        int numOfQueries = 3;
        qg.constructWorkload(templates, numOfQueries);
    }


}
