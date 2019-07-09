package edu.uci.ics.tippers.data;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.db.MySQLResult;
import edu.uci.ics.tippers.model.query.QueryStatement;
import edu.uci.ics.tippers.model.tippers.Infrastructure;
import edu.uci.ics.tippers.model.tippers.User;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class QueryGeneration {

    private List<Infrastructure> infras;
    private List<User> users;
    private MySQLQueryManager mySQLQueryManager = new MySQLQueryManager();
    private Connection connection = MySQLConnectionManager.getInstance().getConnection();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private List<Double> hours = new ArrayList<Double>(Arrays.asList(500.0, 1000.0, 2000.0, 5000.0, 10000.0));
    private List<Integer> userPreds = new ArrayList<Integer>(Arrays.asList(500, 1000, 2000));
    private List<Integer> locPreds = new ArrayList<Integer>(Arrays.asList(5, 10, 15, 20, 30, 50));


    public QueryGeneration() {
        infras = DataGeneration.getAllInfra();
        users = DataGeneration.getAllUser();
        Random r = new Random();
    }

    private long getRandomTimeBetweenTwoDates() {
        long diff = Timestamp.valueOf(PolicyConstants.END_TS).getTime() -
                Timestamp.valueOf(PolicyConstants.START_TS).getTime() + 1;
        return Timestamp.valueOf(PolicyConstants.START_TS).getTime() + (long) (Math.random() * diff);
    }

    private Timestamp getRandomTimeStamp() {
        LocalDateTime randomDate = Instant.ofEpochMilli(getRandomTimeBetweenTwoDates()).atZone(ZoneId.systemDefault()).toLocalDateTime();
        return Timestamp.valueOf(randomDate);
    }

    private Timestamp getEndingTimeInterval(Timestamp timestamp) {
        if (timestamp == null)
            return getRandomTimeStamp();
        int hourIndex = new Random().nextInt(hours.size());
        double rHour = hours.get(hourIndex);

        rHour = rHour * Math.random();
        long milliseconds = (long) (rHour * 60.0 * 60.0 * 1000.0);
        return new Timestamp(timestamp.getTime() + milliseconds);
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
        MySQLResult mySQLResult = mySQLQueryManager.runTimedQueryWithSorting(query, true);
        return (float) mySQLResult.getResultCount() / (float) PolicyConstants.NUMBER_OR_TUPLES;
    }


    private QueryStatement createQuery1() {
        String query, selType = "";
        float selQuery = 0;
        do {
            String user = String.valueOf(users.get(new Random().nextInt(users.size())).getUser_id());
            Timestamp startTS = getRandomTimeStamp();
            String start = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(startTS);
            String end = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(getEndingTimeInterval(startTS));
            query = String.format(" (user_id = %s) AND (timeStamp >= \"%s\") AND (timeStamp <= \"%s\")", user, start, end);
            selQuery = checkSelectivity(query);
            selType = checkSelectivityType(selQuery);
        } while (selType.isEmpty());
        return new QueryStatement(query, 1, selQuery, selType,
                new Timestamp(System.currentTimeMillis()));
    }

    private QueryStatement createQuery2(int elemCount, boolean user) {
        int c = 0;
        String query, selType = "";
        float selQuery = 0;
        do {
            Timestamp startTS = getRandomTimeStamp();
            String start = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(startTS);
            String end = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(getEndingTimeInterval(startTS));
            query = String.format("timeStamp >= \"%s\" AND timeStamp <= \"%s\" ", start, end);
            List<String> preds = new ArrayList<>();
            while (c < elemCount) {
                if (user)
                    preds.add(String.valueOf(users.get(new Random().nextInt(users.size())).getUser_id()));
                else
                    preds.add(String.valueOf(infras.get(new Random().nextInt(infras.size())).getName()));
                c += 1;
            }
            if (user) query += "AND user_id in ( ";
            else query += "AND location_id in ( ";
            query += preds.stream().map(item -> item + ", ").collect(Collectors.joining(" "));
            query = query.substring(0, query.length() - 2); //removing the extra comma
            query += ")";
            selQuery = checkSelectivity(query);
            selType = checkSelectivityType(selQuery);
            c = 0;
        } while (!selType.equalsIgnoreCase("high"));
        return new QueryStatement(query, 2, selQuery, selType, new Timestamp(System.currentTimeMillis()));
    }


    private List<QueryStatement> getQueries(int templateNum, int numOfQueries) {
        List<QueryStatement> queries = new ArrayList<>();
        int count = 0;
        while (count < numOfQueries) {
            count += 1;
            if (templateNum == 0) queries.add(createQuery1());
            else if (templateNum == 1) {
                int userCount = userPreds.get(new Random().nextInt(userPreds.size()));
                queries.add(createQuery2(userCount, true));
                System.out.println("Generated query " + count);
            } else if (templateNum == 2) {
                int locCount = locPreds.get(new Random().nextInt(locPreds.size()));
                queries.add(createQuery2(locCount, false));
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
//        DataGeneration dataGeneration = new DataGeneration();
//        dataGeneration.runScript("mysql/menagerie.sql");

        List<QueryStatement> queries = new ArrayList<>();
        for (int i = 0; i < templates.length; i++) {
            if (templates[i]) queries.addAll(getQueries(i, numOfQueries));
        }
        insertQuery(queries);
    }


}
