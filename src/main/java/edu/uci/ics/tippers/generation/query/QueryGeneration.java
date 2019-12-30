package edu.uci.ics.tippers.generation.query;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.db.MySQLResult;
import edu.uci.ics.tippers.generation.policy.PolicyGen;
import edu.uci.ics.tippers.model.policy.TimeStampPredicate;
import edu.uci.ics.tippers.model.query.QueryStatement;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class QueryGeneration {

    private List<Integer> user_ids;
    private List<String> locations;
    Timestamp start_beg, start_fin;
    private MySQLQueryManager mySQLQueryManager;
    private Connection connection = MySQLConnectionManager.getInstance().getConnection();
    private List<Integer> hours;
    private List<Integer> numUsers;
    private List<Integer> numLocs;

    private PolicyGen pg; //helper methods defined in this class

    public QueryGeneration() {
        pg = new PolicyGen();
        this.user_ids = pg.getAllUsers(false);
        this.locations = pg.getAllLocations();
        this.start_beg = pg.getDate("MIN");
        this.start_fin = pg.getDate("MAX");

        hours = new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5, 7, 10, 12, 15, 17, 20, 23));
        numUsers = new ArrayList<Integer>(Arrays.asList(10, 20, 30, 40, 50, 75, 100, 200, 300, 500, 750, 1000, 1250, 1500, 2000));
        numLocs = new ArrayList<Integer>(Arrays.asList(1, 2, 3, 5, 10, 15, 20, 30, 50));

        this.mySQLQueryManager = new MySQLQueryManager();
        Random r = new Random();
    }

    private String checkSelectivityType(double selectivity) {
        String selType = null;
        double lowSelDown = 0.0001, lowSelUp = 0.009;
        double medSelDown = 0.009, medSelUp = 0.09;
        double highSelDown = 0.09, highSelUp = 0.5;
        if (lowSelDown < selectivity && selectivity < lowSelUp) selType = "low";
        if (medSelDown < selectivity && selectivity < medSelUp) selType = "medium";
        if (highSelDown < selectivity && selectivity < highSelUp) selType = "high";
        return selType;
    }

    /**
     * @param query
     * @return
     */
    private float checkSelectivity(String query) {
        MySQLResult mySQLResult = mySQLQueryManager.runTimedQueryWithOutSorting(query, true);
        return (float) mySQLResult.getResultCount() / (float) PolicyConstants.NUMBER_OR_TUPLES;
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
                float selQuery = checkSelectivity(query);
                String querySelType = checkSelectivityType(selQuery);
                if(querySelType == null){
                    if (duration < 1439) duration += 30;
                    else if (locFlag && i++ < numLocs.size()) {
                        locs = numLocs.get(i);
                        locFlag = false;
                    } else if (!locFlag && days < 90) {
                        days += 1;
                        locFlag = true;
                    }
                    continue;
                }
                if(selType.equalsIgnoreCase(querySelType)) {
                    queries.add(new QueryStatement(query, 1, selQuery, querySelType,
                            new Timestamp(System.currentTimeMillis())));
                    numQ++;
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
                        else if (locFlag && i++ < numLocs.size()) {
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
                        else if (locFlag && i++ < numLocs.size()) {
                            locs = numLocs.get(i);
                            locFlag = false;
                        } else if (!locFlag && days < 90) {
                            days += 1;
                            if(days % 5 == 0) locFlag = true;
                        }
                    }
                }
            } while (numQ < queryCount);
        }
        return queries;
    }

    /**
     * Query 1: Select * from PRESENCE P1 where P1.user_id in [......]
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
                float selQuery = checkSelectivity(query);
                String querySelType = checkSelectivityType(selQuery);
                if(querySelType == null){
                    if (duration < 1439) duration += 100;
                    else if (userFlag && i++ < numUsers.size()) {
                        userCount = numUsers.get(i);
                        userFlag = false;
                    } else if (!userFlag && days < 90) {
                        days += 1;
                        if(days % 5 == 0) userFlag = true;
                    }
                    continue;
                }
                if(selType.equalsIgnoreCase(querySelType)) {
                    queries.add(new QueryStatement(query, 2, selQuery, querySelType,
                            new Timestamp(System.currentTimeMillis())));
                    numQ++;
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
                        else if (userFlag && i++ < numUsers.size()) {
                            userCount = numUsers.get(i);
                            userFlag = false;
                        } else if (!userFlag && days < 90) {
                            days += 1;
                            if(days % 10 == 0) userFlag = true;
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
                        else if (userFlag && i++ < numUsers.size()) {
                            userCount = numUsers.get(i);
                            userFlag = false;
                        } else if (!userFlag && days < 90) {
                            days += 1;
                            if(days % 20 == 0) userFlag = true;
                        }
                    }
                }
            } while (numQ < queryCount);
        }
        return queries;
    }



    private List<QueryStatement> getQueries(int templateNum, List<String> selTypes, int numOfQueries) {
        if (templateNum == 0) {
            return createQuery1(selTypes, numOfQueries);
        } else if (templateNum == 1) {
            return createQuery2(selTypes, numOfQueries);
        } else if (templateNum == 2) {
            return null;
        } else if (templateNum == 3) {
            return null;
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
                        "WHERE q.template = ? limit " + query_count);
                queryStm.setInt(1, template);
            }
            else {
                queryStm = connection.prepareStatement("SELECT id, query_statement, selectivity FROM queries as q " +
                        "WHERE q.selectivity_type = ? AND q.template = ? limit " + query_count);
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
        boolean[] templates = {true, true, false, false};
        int numOfQueries = 10;
        qg.constructWorkload(templates, numOfQueries);
    }


}
