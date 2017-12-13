package edu.uci.ics.tippers.data;

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import edu.uci.ics.tippers.model.query.BasicQuery;
import edu.uci.ics.tippers.model.tippers.Infrastructure;
import edu.uci.ics.tippers.model.tippers.User;

import java.io.IOException;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;
import java.io.File;

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
}
