package edu.uci.ics.tippers.data;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.db.MySQLResult;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;
import edu.uci.ics.tippers.model.query.RangeQuery;
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

    List<Infrastructure> infras;
    List<User> users;
    Random r;
    Writer writer;
    MySQLQueryManager mySQLQueryManager = new MySQLQueryManager();
    Connection connection = MySQLConnectionManager.getInstance().getConnection();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    List<Double> selectivity = new ArrayList<Double>(Arrays.asList(0.0001, 0.01));
    List<Double> hours = new ArrayList<Double>(Arrays.asList(24.0, 48.0, 72.0, 96.0, 120.0, 200.0, 300.0, 500.0));


    public QueryGeneration(){
        infras = DataGeneration.getAllInfra();
        users = DataGeneration.getAllUser();
        r = new Random();
        writer = new Writer();

    }

    private long getRandomTimeBetweenTwoDates () {
        long diff = Timestamp.valueOf(PolicyConstants.END_TS).getTime() -
                Timestamp.valueOf(PolicyConstants.START_TS).getTime() + 1;
        return Timestamp.valueOf(PolicyConstants.START_TS).getTime() + (long) (Math.random() * diff);
    }

    private Timestamp getRandomTimeStamp() {
        LocalDateTime randomDate = Instant.ofEpochMilli(getRandomTimeBetweenTwoDates()).atZone(ZoneId.systemDefault()).toLocalDateTime();
        return Timestamp.valueOf(randomDate);
    }

    private Timestamp getEndingTimeInterval(Timestamp timestamp){
        if (timestamp == null)
            return getRandomTimeStamp();
        int hourIndex = new Random().nextInt(hours.size());
        double rHour = hours.get(hourIndex);

        rHour = rHour * Math.random();
        Long milliseconds = (long)(rHour * 60.0 * 60.0 * 1000.0);
        return new Timestamp(timestamp.getTime() + milliseconds);
    }

    private boolean checkSelectivity(String query, double selectivity){
        MySQLResult mySQLResult = mySQLQueryManager.runTimedQueryWithResultCount(query);
        double selQuery = (double) mySQLResult.getResultCount()/(double)PolicyConstants.NUMBER_OR_TUPLES;
        return selQuery > selectivity/100 && selQuery < selectivity*100;
    }


    private String createQuery1(double selectivity){
        String query;
        do{
            String user = String.valueOf(users.get(new Random().nextInt(users.size())).getUser_id());
            Timestamp startTS = getRandomTimeStamp();
            String start = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(startTS);
            String end = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(getEndingTimeInterval(startTS));
            query = String.format(" (user_id = %s) AND (timeStamp >= \"%s\") AND (timeStamp <= \"%s\")", user, start, end);
        } while(!(checkSelectivity(query, selectivity)));
        return query;
    }

    private String createQuery2(int elemCount, double selectivity, boolean user){
        int c = 0;
        String query;
        do{
            Timestamp startTS = getRandomTimeStamp();
            String start = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(startTS);
            String end = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(getEndingTimeInterval(startTS));
            query = String.format("timeStamp >= \"%s\" AND timeStamp <= \"%s\" ", start, end);
            List<String> preds = new ArrayList<>();
            while(c < elemCount){
                if(user)
                    preds.add(String.valueOf(users.get(new Random().nextInt(users.size())).getUser_id()));
                else
                    preds.add(String.valueOf(infras.get(new Random().nextInt(infras.size())).getName()));
                c += 1;
            }
            if(user) query += "AND user_id in ( ";
            else query += "AND location_id in ( ";
            query += preds.stream().map(item -> item + ", " ).collect(Collectors.joining(" "));
            query = query.substring(0, query.length()-2); //removing the extra comma
            query += ")";
            c = 0;
        } while (!checkSelectivity(query, selectivity));
        return query;
    }


    private List<String> getQueries(int templateNum, int numOfQueries){
        List<String> queries = new ArrayList<>();
        for (int i = 0; i < selectivity.size(); i++) {
            int count = 0;
            double sel = selectivity.get(i);
            while(count < numOfQueries){
                count += 1;
                if(templateNum == 0) queries.add(createQuery1(sel));
                else if(templateNum == 1) {
                    int [] numUsers = {50};
                    int userCount = numUsers[(new Random().nextInt(numUsers.length))];
                    queries.add(createQuery2(userCount, sel, true));
                }
                else if(templateNum == 2){
                    int [] numLocations = {15};
                    int locCount = numLocations[(new Random().nextInt(numLocations.length))];
                    queries.add(createQuery2(locCount, sel, false));
                }
            }
        }
        return queries;
    }

    public void constructWorkload(boolean [] templates, int numOfQueries){
        List<String> queries = new ArrayList<>();
        for (int i = 0; i < templates.length; i++) {
            if(templates[i]) queries.addAll(getQueries(i, numOfQueries));
        }
        writer.writeToFile(queries, "queries.txt", PolicyConstants.BE_POLICY_DIR);
    }
}
