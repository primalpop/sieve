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


    private void writeJSONToFile(List<BasicQuery> basicQueries, int numberOfPolicies, int numberOfAttributes){
        ObjectMapper mapper = new ObjectMapper();
        mapper.setDateFormat(formatter);
        ObjectWriter writer = mapper.writer(new DefaultPrettyPrinter());
        try {
            writer.writeValue(new File(PolicyConstants.BASIC_POLICY_DIR + "policy"+numberOfPolicies+".json"), basicQueries);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void generateBasicPolicy(int numberOfPolicies, int numberOfAttributes) {

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
        writeJSONToFile(basicQueries, numberOfPolicies, numberOfAttributes);
    }


    private void writeJSONToFileRangePolicy(List<RangeQuery> rangeQueries, int numberOfPolicies){
        ObjectMapper mapper = new ObjectMapper();
        mapper.setDateFormat(formatter);
        ObjectWriter writer = mapper.writer(new DefaultPrettyPrinter());
        try {
            writer.writeValue(new File(PolicyConstants.RANGE_POLICY_DIR + "policyR-"+numberOfPolicies+".json"), rangeQueries);
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

    /**
     * Attributes are added to the policy based on a coin toss
     * @param numberOfPolicies
     */
    public void generateRangePolicy(int numberOfPolicies){

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

        writeJSONToFileRangePolicy(rangeQueries, numberOfPolicies);

    }
}
