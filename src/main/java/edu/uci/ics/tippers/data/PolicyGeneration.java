package edu.uci.ics.tippers.data;

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.model.query.BasicQuery;
import edu.uci.ics.tippers.model.query.RangeQuery;
import edu.uci.ics.tippers.model.tippers.Infrastructure;
import edu.uci.ics.tippers.model.tippers.User;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

/**
 * Author primpap
 */
public class PolicyGeneration {

    List<Infrastructure> infras;
    List<User> users;
    List<String> activities;

    int lowTemp = 55;
    int highTemp = 75;
    int lowWemo = 0;
    int highWemo = 100;

    String startTS = "2017-03-31 15:10:00 ";
    String endTS = "2017-10-23 12:40:55";

    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static final double[] hours = { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 12.0, 24.0, 48.0, 72.0, 168.0, 336.0};

    Random r = new Random();

    public PolicyGeneration() {

        infras = DataGeneration.getAllInfra();

        users = DataGeneration.getAllUser();

        activities = new ArrayList<String>();
        activities.add("class");
        activities.add("meeting");
        activities.add("seminar");
        activities.add("private");
        activities.add("walking");
        activities.add("unknown");
        activities.add("work");

    }

    private Timestamp getRandomTimeStamp() {
        Calendar cal = Calendar.getInstance();

        try {
            cal.setTime(formatter.parse(startTS));
            Long value1 = cal.getTimeInMillis();
            cal.setTime(formatter.parse(endTS));
            Long value2 = cal.getTimeInMillis();
            long value3 = (long) (value1 + Math.random() * (value2 - value1));
            cal.setTimeInMillis(value3);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return new Timestamp(cal.getTimeInMillis());
    }


    private Timestamp getEndingTimeInterval(Timestamp timestamp){

        int hourIndex = new Random().nextInt(hours.length);
        double rHour = hours[hourIndex];
        
        rHour = rHour * Math.random();
        Long milliseconds = (long)(rHour * 60.0 * 60.0 * 1000.0);
        return new Timestamp(timestamp.getTime() + milliseconds);
    }


    private int getEndingTemperature(int temperature){
        int noise =  ((int) (1 + Math.random() * (4)));

        if (temperature + noise < PolicyConstants.HIGH_TEMPERATURE){
            return temperature + noise;
        }
        else
            return temperature + 1;
    }

    private int getEndingEnergy(int energy){
        int noise =  ((int) (1 + Math.random() * (20)));

        if (energy + noise < PolicyConstants.HIGH_WEMO){
            return energy + noise;
        }
        else
            return energy + 1;
    }


    private void writeJSONToFile(List<BasicQuery> basicQueries, int numberOfPolicies, String policyDir){
        ObjectMapper mapper = new ObjectMapper();
        mapper.setDateFormat(formatter);
        ObjectWriter writer = mapper.writer(new DefaultPrettyPrinter());
        try {
            writer.writeValue(new File(policyDir + "policy"+numberOfPolicies+".json"), basicQueries);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void generateBasicPolicy1(int numberOfPolicies) {

        List<BasicQuery> basicQueries = new ArrayList<BasicQuery>();

        for (int i = 0; i < numberOfPolicies; i++) {
            User user = users.get(new Random().nextInt(users.size()));
            Infrastructure infra = infras.get(new Random().nextInt(infras.size()));
            String temperature = String.valueOf(r.nextInt(highTemp - lowTemp) + lowTemp);
            String wemo = String.valueOf(r.nextInt(highWemo - lowWemo) + lowWemo);
            Timestamp ts = getRandomTimeStamp();
            String activity = activities.get(new Random().nextInt(activities.size()));
            BasicQuery bq = new BasicQuery(String.valueOf(user.getUser_id()), infra.getName(), ts, temperature, wemo, activity);
            basicQueries.add(bq);
        }
        writeJSONToFile(basicQueries, numberOfPolicies, PolicyConstants.BASIC_POLICY_1_DIR);
    }

    public void generateBasicPolicy2(int numberOfPolicies){

        List<BasicQuery> basicQueries = new ArrayList<BasicQuery>();

        for (int i = 0; i < numberOfPolicies; i++) {
            User user = coinFlip()? users.get(new Random().nextInt(users.size())) : null;
            Infrastructure infra = coinFlip()? infras.get(new Random().nextInt(infras.size())): null;
            String temperature = coinFlip()? String.valueOf(r.nextInt(highTemp - lowTemp) + lowTemp): null;
            String wemo = coinFlip()? String.valueOf(r.nextInt(highWemo - lowWemo) + lowWemo): null;
            Timestamp ts = coinFlip()? getRandomTimeStamp(): null;
            String activity = coinFlip()? activities.get(new Random().nextInt(activities.size())): null;

            BasicQuery bq = new BasicQuery();
            if(user != null)
                bq.setUser_id(String.valueOf(user.getUser_id()));
            if(infra != null)
                bq.setLocation_id(infra.getName());
            if (activity != null)
                bq.setActivity(activity);
            if(ts != null)
                bq.setTimestamp(ts);
            if(temperature != null)
                bq.setTemperature(String.valueOf(temperature));
            if(wemo != null)
                bq.setWemo(String.valueOf(wemo));

            basicQueries.add(bq);
        }
        writeJSONToFile(basicQueries, numberOfPolicies, PolicyConstants.BASIC_POLICY_2_DIR);

    }


    private void writeJSONToFileRangePolicy(List<RangeQuery> rangeQueries, int numberOfPolicies, String policyDir){
        ObjectMapper mapper = new ObjectMapper();
        mapper.setDateFormat(formatter);
        ObjectWriter writer = mapper.writer(new DefaultPrettyPrinter());
        try {
            writer.writeValue(new File(policyDir + "policyR-"+numberOfPolicies+".json"), rangeQueries);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean coinFlip(){
        int result = r.nextInt(2);
        if(result == 0) {
            return true;
        }
        else
            return false;
    }

    public void generateRangePolicy1(int numberOfPolicies){

        List<RangeQuery> rangeQueries = new ArrayList<RangeQuery>();

        for (int i = 0; i < numberOfPolicies; i++) {
            User user = users.get(new Random().nextInt(users.size()));
            Infrastructure infra = infras.get(new Random().nextInt(infras.size()));
            String activity = activities.get(new Random().nextInt(activities.size()));
            Timestamp sTS = getRandomTimeStamp();
            Timestamp eTS = getEndingTimeInterval(sTS);
            Integer start_temp = r.nextInt(highTemp - lowTemp) + lowTemp;
            Integer end_temp = getEndingTemperature(start_temp);
            Integer start_wemo =  r.nextInt(highWemo - lowWemo) + lowWemo;
            Integer end_wemo = getEndingEnergy(start_wemo);

            RangeQuery rq = new RangeQuery(sTS, eTS, String.valueOf(start_wemo), String.valueOf(end_wemo), String.valueOf(start_temp),
                    String.valueOf(end_temp), String.valueOf(user.getUser_id()), infra.getName(), activity);

            rangeQueries.add(rq);
        }

        writeJSONToFileRangePolicy(rangeQueries, numberOfPolicies, PolicyConstants.RANGE_POLICY_1_DIR);

    }

    /**
     * Attributes are added to the policy based on a coin toss
     * @param numberOfPolicies
     */
    public void generateRangePolicy2(int numberOfPolicies){

        List<RangeQuery> rangeQueries = new ArrayList<RangeQuery>();

        for (int i = 0; i < numberOfPolicies; i++) {
            User user = coinFlip()? users.get(new Random().nextInt(users.size())) : null;
            Infrastructure infra = coinFlip()? infras.get(new Random().nextInt(infras.size())) : null;
            String activity = coinFlip()? activities.get(new Random().nextInt(activities.size())) : null;
            Timestamp sTS = coinFlip()? getRandomTimeStamp() : null;
            Timestamp eTS = coinFlip()? (sTS != null ? getEndingTimeInterval(sTS): getRandomTimeStamp()):  null;
            Integer start_temp = coinFlip()? (r.nextInt(highTemp - lowTemp) + lowTemp): null;
            Integer end_temp = coinFlip()? (start_temp != null  ? getEndingTemperature(start_temp): r.nextInt(highTemp - lowTemp) + lowTemp): null;
            Integer start_wemo = coinFlip() ? (r.nextInt(highWemo - lowWemo) + lowWemo): null;
            Integer end_wemo = coinFlip()? (start_wemo != null ? getEndingEnergy(start_wemo): r.nextInt(highWemo - lowWemo) + lowWemo): null;

            RangeQuery rq = new RangeQuery();
            if(user != null)
                rq.setUser_id(String.valueOf(user.getUser_id()));
            if(infra != null)
                rq.setLocation_id(infra.getName());
            if (activity != null)
                rq.setActivity(activity);
            if(sTS != null)
                rq.setStart_timestamp(sTS);
            if(eTS != null)
                rq.setEnd_timestamp(eTS);
            if(start_temp != null)
                rq.setStart_temp(String.valueOf(start_temp));
            if(end_temp != null)
                rq.setEnd_temp(String.valueOf(end_temp));
            if(start_wemo != null)
                rq.setStart_wemo(String.valueOf(start_wemo));
            if(end_wemo != null)
                rq.setEnd_wemo(String.valueOf(end_wemo));

            rangeQueries.add(rq);
        }

        writeJSONToFileRangePolicy(rangeQueries, numberOfPolicies, PolicyConstants.RANGE_POLICY_2_DIR);

    }


    /**
     * Guard based only indexed attributes: {<user_id, timeStamp, location_id>, <user_id, timeStamp>}
     * @param rangeQueries
     * @return
     */
    public String generateGuard(List<RangeQuery> rangeQueries){

        StringBuilder guard = new StringBuilder();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        for (RangeQuery rq: rangeQueries) {
            if(rq.getUser_id() != null && (rq.getStart_timestamp() != null || rq.getEnd_timestamp() != null)){
                if(guard.length() > 1) guard.append(" OR ");
                guard.append("(");
                guard.append("user_id = ");
                guard.append("\'").append(rq.getUser_id()).append("\'");
                if(rq.getStart_timestamp()!= null) {
                    guard.append(" AND ");
                    guard.append("timeStamp >= ");
                    guard.append("\'").append(rq.getStart_timestamp()).append("\'");
                }
                if (rq.getEnd_timestamp()!= null) {
                    guard.append(" AND ");
                    guard.append("timeStamp <= ");
                    guard.append("\'").append(rq.getEnd_timestamp()).append("\'");
                }
                if(rq.getLocation_id() != null){
                    guard.append(" AND ");
                    guard.append("location_id = ");
                    guard.append("\'").append(rq.getLocation_id()).append("\'");
                }
                guard.append(")");
            }
        }

        return guard.toString();
    }
}
