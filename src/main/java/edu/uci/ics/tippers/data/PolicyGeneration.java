package edu.uci.ics.tippers.data;

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.model.query.BasicQuery;
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

    private static final int[] hours = { 1, 2, 3, 4, 5, 6, 8, 10, 12, 24, 48, 72, 168, 336};
    

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

    public Timestamp getRandomTimeStamp() {
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


    public Timestamp getEndingTimeInterval(Timestamp timestamp){

        int hourIndex = new Random().nextInt(hours.length);
        int rHour = hours[hourIndex];

        Random random = new Random();
        int noise = random.nextInt(1001) / 1000;

        rHour *= noise;
        Long milliseconds = Long.valueOf(rHour * 60 * 60 * 1000);
        return new Timestamp(timestamp.getTime() + milliseconds);
    }


    public int getEndingTemperature(int temperature){
        Random random = new Random();
        int noise =  ((int) (1 + Math.random() * (4)));
        
        if (temperature + noise <PolicyConstants.HIGH_TEMPERATURE){
            return temperature + noise;
        }
        else
            return temperature;
    }

    public int getEndingEnergy(int energy){
        Random random = new Random();
        int noise =  ((int) (1 + Math.random() * (20)));

        if (energy + noise <PolicyConstants.HIGH_WEMO){
            return energy + noise;
        }
        else
            return energy;
    }


    public void writeJSONToFile(List<BasicQuery> basicQueries, int numberOfPolicies, int numberOfAttributes){
        ObjectMapper mapper = new ObjectMapper();
        mapper.setDateFormat(formatter);
        ObjectWriter writer = mapper.writer(new DefaultPrettyPrinter());
        try {
            writer.writeValue(new File(PolicyConstants.POLICY_DIR + "policy"+numberOfPolicies+".json"), basicQueries);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void generateRandomPolicy(int numberOfPolicies, int numberOfAttributes) {

        Random r = new Random();

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


    public void generateRangePolicy(int numberOfPolicies, int numberOfAttributes){

    }
}
