package edu.uci.ics.tippers.generation.query;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.db.MySQLResult;
import edu.uci.ics.tippers.generation.policy.PolicyGen;
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
    Timestamp end_beg, end_fin;
    private MySQLQueryManager mySQLQueryManager;
    private Connection connection = MySQLConnectionManager.getInstance().getConnection();
    SimpleDateFormat sdf;
    private List<Double> hours;
    private List<Integer> numUsers;
    private List<Integer> numLocs;

    private PolicyGen pg; //helper methods defined in this class

    public QueryGeneration() {
        pg = new PolicyGen();
        this.user_ids = pg.getAllUsers();
        this.locations = pg.getAllLocations();
        this.start_beg = pg.getTimestamp(PolicyConstants.START_TIMESTAMP_ATTR, "MIN");
        this.start_fin = pg.getTimestamp(PolicyConstants.START_TIMESTAMP_ATTR, "MAX");
        this.end_beg = pg.getTimestamp(PolicyConstants.FINISH_TIMESTAMP_ATTR, "MIN");
        this.end_fin = pg.getTimestamp(PolicyConstants.FINISH_TIMESTAMP_ATTR, "MAX");

        hours = new ArrayList<Double>(Arrays.asList(10.0, 20.0, 30.0, 40.0, 50.0, 100.0, 200.0, 300.0, 500.0, 1000.0, 2000.0, 3000.0, 5000.0, 7000.0));
        numUsers = new ArrayList<Integer>(Arrays.asList(500, 1000, 2000));
        numLocs = new ArrayList<Integer>(Arrays.asList(1, 2, 3, 5, 10, 15, 20, 30, 50, 75, 100, 150, 200));

        this.mySQLQueryManager = new MySQLQueryManager();
        this.sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Random r = new Random();
    }

    private String checkSelectivityType(double selectivity) {
        String selType = "";
        double lowSelDown = 0.00001, lowSelUp = 0.0001;
        double medSelDown = 0.0001, medSelUp = 0.01;
        double highSelDown = 0.01, highSelUp = 0.3;
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

//    /**
//     * Get location(s) of a user within a period of time
//     * Query 1: Select * from SEMANTIC_OBSERVATION where user_id = x and timeStamp >= y and timeStamp <= z
//     * @return
//     */
//    private QueryStatement createQuery1() {
//        String query, selType = "";
//        float selQuery = 0;
//        do {
//            int user = user_ids.get(new Random().nextInt(user_ids.size()));
//            Timestamp startTS = getRandomTimeStamp();
//            String start = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(startTS);
//            String end = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(getEndingTimeInterval(startTS));
//            query = String.format(" (user_id = %s) AND (timeStamp >= \"%s\") AND (timeStamp <= \"%s\")", user, start, end);
//            selQuery = checkSelectivity(query);
//            selType = checkSelectivityType(selQuery);
//        } while (selType.isEmpty());
//        return new QueryStatement(query, 1, selQuery, selType,
//                new Timestamp(System.currentTimeMillis()));
//    }

    /**
     * Query 1: Select * from PRESENCE where location_id in [......]
     * and start <= t2 and end >= t1
     * The algorithm iterates from low to high selectivity types and
     * progressively increases the number of location predicates and/or time range
     * to satisfy the required selectivity except when the query is jumping ahead
     * in selectivity (e.g., chosen selType = low, generated query = medium) in which
     * case it waits instead.
     * @param selTypes - types of query selectivity needed
     * @param queryCount - number of each queries of each selectivity type
     * @return
     */
    private List<QueryStatement> createQuery1(List<String> selTypes, int queryCount) {
        List<QueryStatement> queries = new ArrayList<>();
        int i = 0, j = 0;
        Timestamp startTS = pg.start_beg; //pg.getRandomTimeStamp(PolicyConstants.START_TIMESTAMP_ATTR);
        String start = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(startTS);
        for (int k = 0; k < selTypes.size(); k++) {
            int numQ = 0;
            int locs = numLocs.get(i);
            double extension = hours.get(j);
            do {
                Timestamp finishTS = pg.getEndingTimeWithExtension(startTS, extension);
                String finish = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(finishTS);
                String query = String.format("start >= \"%s\" AND finish <= \"%s\" ", start, finish);
                List<String> locPreds = new ArrayList<>();
                int predCount = 0;
                while (predCount < locs) {
                    locPreds.add(String.valueOf(locations.get(new Random().nextInt(locations.size()))));
                    predCount += 1;
                }
                query += "AND location_id in ( ";
                query += locPreds.stream().map(item -> item + ", ").collect(Collectors.joining(" "));
                query = query.substring(0, query.length() - 2); //removing the extra comma
                query += ")";
                float selQuery = checkSelectivity(query);
                String selType = checkSelectivityType(selQuery);
                if(!selType.equalsIgnoreCase(selTypes.get(k))){
                    if((k == selTypes.size()-1) || ((k+1) < selTypes.size() && !selType.equalsIgnoreCase(selTypes.get(k+1)))) {
                        if(i == j && i++ < locations.size()) locs = numLocs.get(i);
                        else if(i >=j && j++ < hours.size()) extension = hours.get(j);
                        else if (i > locations.size() || j > hours.size()) {
                            System.out.println("Stopped at " + selTypes.get(k) + " with " + numQ + " queries");
                            return queries;
                        }
                    }
                }
                else{
                    queries.add(new QueryStatement(query, 2, selQuery, selType,
                            new Timestamp(System.currentTimeMillis())));
                    numQ++;
                }
            } while (numQ < queryCount);
        }
        return queries;
    }


    private List<QueryStatement> getQueries(int templateNum, List<String> selTypes, int numOfQueries) {
        if (templateNum == 0) {
            return createQuery1(selTypes, numOfQueries);
        } else if (templateNum == 1) {
            return null;
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


    public List<QueryStatement> retrieveQueries(String selectivity_type, int query_count){
        List<QueryStatement> queryStatements = new ArrayList<>();
        PreparedStatement queryStm = null;
        try {
            queryStm = connection.prepareStatement("SELECT id, query_statement FROM menagerie.queries as q " +
                    "WHERE q.selectivity_type = ? limit " + query_count);
            queryStm.setString(1, selectivity_type);
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) {
                QueryStatement qs = new QueryStatement();
                qs.setQuery(rs.getString("query_statement"));
                qs.setId(rs.getInt("id"));
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

    public static void main(String [] args){
        QueryGeneration qg = new QueryGeneration();
        boolean [] templates = {true, false, false, false};
        int numOfQueries = 2;
        qg.constructWorkload(templates, numOfQueries);
    }


}
