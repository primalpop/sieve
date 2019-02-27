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

public class QueryGeneration {

    List<Infrastructure> infras;
    List<User> users;
    Random r;
    Writer writer;
    MySQLQueryManager mySQLQueryManager = new MySQLQueryManager();
    Connection connection = MySQLConnectionManager.getInstance().getConnection();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    List<Double> selectivity = new ArrayList<Double>(Arrays.asList(0.001, 0.002, 0.003));
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


    private String createQuery1(double selectivity){
        double selOfPolicy = 0.0;
        List<ObjectCondition> objectConditions;
        do {
            RangeQuery rq = new RangeQuery();
            rq.setUser_id(String.valueOf(users.get(new Random().nextInt(users.size())).getUser_id()));
            rq.setStart_timestamp(getRandomTimeStamp());
            rq.setEnd_timestamp(getEndingTimeInterval(rq.getStart_timestamp()));
            objectConditions = rq.createObjectCondition(-1);
            selOfPolicy = BEPolicy.computeL(objectConditions);
        } while (!(selOfPolicy > selectivity/1000 && selOfPolicy < selectivity));
        BEPolicy dummyPolicy = new BEPolicy(objectConditions);
        return dummyPolicy.cleanQueryFromObjectConditions();
    }

    private String createQuery2(int elemCount, double selectivity, boolean user){
        BEExpression dummyExp = new BEExpression();
        double selOfExp = 0.0;
        int c = 0;
        do{
            RangeQuery rq = new RangeQuery();
            rq.setStart_timestamp(getRandomTimeStamp());
            rq.setEnd_timestamp(getEndingTimeInterval(rq.getStart_timestamp()));
            while(c < elemCount){
                List<ObjectCondition> objectConditions;
                if(user) rq.setUser_id(String.valueOf(users.get(new Random().nextInt(users.size())).getUser_id()));
                else rq.setLocation_id(String.valueOf(infras.get(new Random().nextInt(infras.size())).getName()));
                objectConditions = rq.createObjectCondition(-1);
                BEPolicy dummyPolicy = new BEPolicy(objectConditions);
                dummyExp.getPolicies().add(dummyPolicy);
                c += 1;
            }
            selOfExp = dummyExp.computeL();
            c = 0;
        } while (!(selOfExp > selectivity/1000 && selOfExp < selectivity));
        return dummyExp.cleanQueryFromPolices();
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
                    int [] numUsers = {5, 10, 15, 20, 25, 30, 50};
                    int userCount = numUsers[(new Random().nextInt(numUsers.length))];
                    queries.add(createQuery2(userCount, sel, true));
                }
                else if(templateNum == 2){
                    int [] numLocations = {3, 6, 9, 10, 15, 20};
                    int locCount = numLocations[(new Random().nextInt(numLocations.length))];
                    queries.add(createQuery2(locCount, sel, false));
                }
            }
        }
        return queries;
    }

    public List<String> constructWorkload(boolean [] templates, int numOfQueries){
        List<String> queries = new ArrayList<>();
        for (int i = 0; i < templates.length; i++) {
            if(templates[i]) queries.addAll(getQueries(i, numOfQueries));
        }
        return queries;
    }
}
