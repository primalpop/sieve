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
    private List<Integer> userPreds;
    private List<Integer> locPreds;

    private PolicyGen pg; //helper methods defined in this class

    public QueryGeneration() {
        pg = new PolicyGen();
        this.user_ids = pg.getAllUsers();
        this.locations = pg.getAllLocations();
        this.start_beg = pg.getTimestamp(PolicyConstants.START_TIMESTAMP_ATTR, "MIN");
        this.start_fin = pg.getTimestamp(PolicyConstants.START_TIMESTAMP_ATTR, "MAX");
        this.end_beg = pg.getTimestamp(PolicyConstants.END_TIMESTAMP_ATTR, "MIN");
        this.end_fin = pg.getTimestamp(PolicyConstants.END_TIMESTAMP_ATTR, "MAX");

        hours = new ArrayList<Double>(Arrays.asList(500.0, 1000.0, 2000.0, 5000.0, 10000.0));
        userPreds = new ArrayList<Integer>(Arrays.asList(500, 1000, 2000));
        locPreds = new ArrayList<Integer>(Arrays.asList(5, 10, 15, 20, 30, 50));

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
     * @param elemCount - number of predicates for location
     * @return
     */
    private QueryStatement createQuery1(int elemCount) {
        int c = 0;
        String query, selType = "";
        float selQuery = 0;
        do {
            Timestamp startTS = pg.getRandomTimeStamp(PolicyConstants.START_TIMESTAMP_ATTR);
            Timestamp finishTS = pg.getEndingTimeInterval(startTS, hours);
            String start = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(startTS);
            String finish = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(finishTS);
            query = String.format("start <= \"%s\" AND finish >= \"%s\" ", start, finish);
            List<String> preds = new ArrayList<>();
            while (c < elemCount) {
                preds.add(String.valueOf(locations.get(new Random().nextInt(locations.size()))));
                c += 1;
            }
            query += "AND location_id in ( ";
            query += preds.stream().map(item -> item + ", ").collect(Collectors.joining(" "));
            query = query.substring(0, query.length() - 2); //removing the extra comma
            query += ")";
            selQuery = checkSelectivity(query);
            selType = checkSelectivityType(selQuery);
            c = 0;
        } while (!selType.equalsIgnoreCase("high"));
        return new QueryStatement(query, 2, selQuery, selType, new Timestamp(System.currentTimeMillis()));
    }


    private List<QueryStatement> getQueries(int templateNum, String selType, int numOfQueries) {
        List<QueryStatement> queries = new ArrayList<>();
        int count = 0;
        while (count < numOfQueries) {
            count += 1;
            if (templateNum == 0) {
                int locCount = locPreds.get(new Random().nextInt(locPreds.size()));
                queries.add(createQuery1(locCount));
            }
            else if (templateNum == 1) {
                System.out.println("Generated query " + count);
            }
            else if (templateNum == 2) {
                System.out.println("Generated query " + count);
            }
            else if (templateNum == 3) {
                System.out.println("Generated query " + count);
            }
        }

        return queries;
    }

    private void insertQuery(List<QueryStatement> queryStatements) {
        String soInsert = "INSERT INTO menagerie.queries " +
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

        List<QueryStatement> queries = new ArrayList<>();
        for (int i = 0; i < templates.length; i++) {
            if (templates[i]) queries.addAll(getQueries(i, numOfQueries));
        }
        insertQuery(queries);
    }


}
