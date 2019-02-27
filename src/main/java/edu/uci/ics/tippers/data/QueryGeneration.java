package edu.uci.ics.tippers.data;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.db.MySQLResult;
import edu.uci.ics.tippers.fileop.Writer;
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
import java.util.List;
import java.util.Random;

public class QueryGeneration {

    List<Infrastructure> infras;
    List<User> users;
    Random r;
    Writer writer;
    MySQLQueryManager mySQLQueryManager = new MySQLQueryManager();
    Connection connection = MySQLConnectionManager.getInstance().getConnection();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");



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
        int hourIndex = new Random().nextInt(PolicyConstants.HOUR_EXTENSIONS.size());
        double rHour = PolicyConstants.HOUR_EXTENSIONS.get(hourIndex);

        rHour = rHour * Math.random();
        Long milliseconds = (long)(rHour * 60.0 * 60.0 * 1000.0);
        return new Timestamp(timestamp.getTime() + milliseconds);
    }

    public List<String> constructWorkload(boolean [] templates, List<Double> selectivity, int numOfQueries){
        List<String> queries = new ArrayList<>();
        if(templates[0]){
            for (int i = 0; i < selectivity.size(); i++) {
                int count = 0;
                double sel = selectivity.get(i);
                while(count < numOfQueries){
                    count += 1;
                    double selOfPolicy = 0.0;
                    List<ObjectCondition> objectConditions;
                    do {
                        RangeQuery rq = new RangeQuery();
                        rq.setUser_id(String.valueOf(users.get(new Random().nextInt(users.size())).getUser_id()));
                        rq.setStart_timestamp(getRandomTimeStamp());
                        rq.setEnd_timestamp(getEndingTimeInterval(rq.getStart_timestamp()));
                        objectConditions = rq.createObjectCondition(-1);
                        selOfPolicy = BEPolicy.computeL(objectConditions);
                    } while (!(selOfPolicy > sel/10 && selOfPolicy < sel*10));
                    BEPolicy dummyPolicy = new BEPolicy(objectConditions);
                    queries.add(dummyPolicy.createQueryFromObjectConditions());
                }
            }
        }
        if(templates[1]){
            int [] numUsers = {5, 10, 15, 20, 25, 30, 50};
            for (int i = 0; i < selectivity.size(); i++) {
                int count = 0;
                double sel = selectivity.get(i);
                while(count < numOfQueries){
                    count += 1;
                    double selOfPolicy = 0.0;
                    List<ObjectCondition> objectConditions;
                    do {
                        RangeQuery rq = new RangeQuery();
                        rq.setUser_id(String.valueOf(users.get(new Random().nextInt(users.size())).getUser_id()));
                        rq.setStart_timestamp(getRandomTimeStamp());
                        rq.setEnd_timestamp(getEndingTimeInterval(rq.getStart_timestamp()));
                        objectConditions = rq.createObjectCondition(-1);
                        selOfPolicy = BEPolicy.computeL(objectConditions);
                    } while (!(selOfPolicy > sel/10 && selOfPolicy < sel*10));
                    BEPolicy dummyPolicy = new BEPolicy(objectConditions);
                    queries.add(dummyPolicy.createQueryFromObjectConditions());
                }
            }
        }
        if(templates[2]){
            int [] numLocations = {3, 6, 9, 10, 15, 20};

        }
        if(templates[3]){

        }
        return queries;
    }
}
