package edu.uci.ics.tippers.data;

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;
import edu.uci.ics.tippers.model.query.BasicQuery;
import edu.uci.ics.tippers.model.query.RangeQuery;
import edu.uci.ics.tippers.model.tippers.Infrastructure;
import edu.uci.ics.tippers.model.tippers.User;
import org.apache.commons.beanutils.PropertyUtils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.security.Policy;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

import static edu.uci.ics.tippers.common.PolicyConstants.START_TS;

/**
 * Author primpap
 */


public class PolicyGeneration {

    List<Infrastructure> infras;
    List<User> users;
    Random r;
    Writer writer;

    public PolicyGeneration() {

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


    private int getTemperature(String temp){

        int temperature;
        if(temp == null)
            return r.nextInt(PolicyConstants.HIGH_TEMPERATURE - PolicyConstants.LOW_TEMPERATURE) + PolicyConstants.LOW_TEMPERATURE;
        else
            temperature = Integer.parseInt(temp);

        int noise =  ((int) (1 + Math.random() * (4)));

        if (temperature + noise < PolicyConstants.HIGH_TEMPERATURE){
            if (temperature + noise > temperature + 3) return temperature + 3;
            return temperature + noise;
        }
        else
            return temperature + 1;
    }

    private int getEnergy(String wemo){

        int energy;
        if(wemo == null)
            return r.nextInt(PolicyConstants.HIGH_WEMO - PolicyConstants.LOW_WEMO) + PolicyConstants.LOW_WEMO;
        else
            energy = Integer.parseInt(wemo);

        int noise =  ((int) (1 + Math.random() * (20)));

        if (energy + noise < PolicyConstants.HIGH_WEMO){
            if (energy + noise > energy + 5) return energy + 5;
            return energy + noise;
        }
        else
            return energy + 1;
    }


    public void generateBasicPolicy1(int numberOfPolicies) {

        List<BasicQuery> basicQueries = new ArrayList<BasicQuery>();

        for (int i = 0; i < numberOfPolicies; i++) {
            User user = users.get(new Random().nextInt(users.size()));
            Infrastructure infra = infras.get(new Random().nextInt(infras.size()));
            String temperature = String.valueOf(r.nextInt(PolicyConstants.HIGH_TEMPERATURE - PolicyConstants.LOW_TEMPERATURE)
                    + PolicyConstants.LOW_TEMPERATURE);
            String wemo = String.valueOf(r.nextInt(PolicyConstants.HIGH_WEMO - PolicyConstants.LOW_WEMO)
                    + PolicyConstants.LOW_WEMO);
            Timestamp ts = getRandomTimeStamp();
            String activity = PolicyConstants.ACTIVITIES.get(new Random().nextInt(PolicyConstants.ACTIVITIES.size()));
            BasicQuery bq = new BasicQuery(String.valueOf(user.getUser_id()), infra.getName(), ts, temperature, wemo, activity);
            basicQueries.add(bq);
        }
        writer.writeJSONToFile(basicQueries, PolicyConstants.BASIC_POLICY_1_DIR, null);
    }


    public void generateBasicPolicy2(int numberOfPolicies) {

        List<BasicQuery> basicQueries = new ArrayList<BasicQuery>();

        for (int i = 0; i < numberOfPolicies; i++) {
            int attrCount = (int) (r.nextGaussian() * 1 + 4); //mean -4, SD - 1
            if(attrCount <= 0 || attrCount > 6) attrCount = 4;
            BasicQuery bq = new BasicQuery();
            Field[] attributes = bq.getClass().getDeclaredFields();
            ArrayList<Field> attrList = new ArrayList<Field>();
            try {
                for (int j = 0; j < attrCount; j++) {
                    Field attribute = attributes[r.nextInt(attributes.length)];
                    if(attrList.contains(attribute)) {
                        j--;
                        continue;
                    }
                    if (attribute.getName().equalsIgnoreCase("user_id")) {
                        PropertyUtils.setSimpleProperty(bq, attribute.getName(), String.valueOf(users.get(new Random().nextInt(users.size())).getUser_id()));
                    } else if (attribute.getName().equalsIgnoreCase("location_id")) {
                        PropertyUtils.setSimpleProperty(bq, attribute.getName(), infras.get(new Random().nextInt(infras.size())).getName());
                    } else if (attribute.getName().equalsIgnoreCase("timestamp")) {
                        PropertyUtils.setSimpleProperty(bq, attribute.getName(), getRandomTimeStamp());
                    } else if (attribute.getName().equalsIgnoreCase("wemo")) {
                        PropertyUtils.setSimpleProperty(bq, attribute.getName(), String.valueOf(r.nextInt(PolicyConstants.HIGH_WEMO - PolicyConstants.LOW_WEMO)
                                + PolicyConstants.LOW_WEMO));
                    } else if (attribute.getName().equalsIgnoreCase("activity")) {
                        PropertyUtils.setSimpleProperty(bq, attribute.getName(), PolicyConstants.ACTIVITIES.get(new Random().nextInt(PolicyConstants.ACTIVITIES.size())));
                    } else if (attribute.getName().equalsIgnoreCase("temperature")) {
                        PropertyUtils.setSimpleProperty(bq, attribute.getName(), String.valueOf(r.nextInt(PolicyConstants.HIGH_TEMPERATURE
                                - PolicyConstants.LOW_TEMPERATURE) + PolicyConstants.LOW_TEMPERATURE));
                    }
                    attrList.add(attribute);
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            }
            basicQueries.add(bq);
        }

        writer.writeJSONToFile(basicQueries, PolicyConstants.BASIC_POLICY_2_DIR, null);
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
            String activity = PolicyConstants.ACTIVITIES.get(new Random().nextInt(PolicyConstants.ACTIVITIES.size()));
            Timestamp sTS = getRandomTimeStamp();
            Timestamp eTS = getEndingTimeInterval(sTS);
            Integer start_temp = r.nextInt((PolicyConstants.HIGH_TEMPERATURE - PolicyConstants.LOW_TEMPERATURE)
                    + (PolicyConstants.LOW_TEMPERATURE));
            Integer end_temp = getTemperature(String.valueOf(start_temp));
            Integer start_wemo =  r.nextInt(PolicyConstants.HIGH_WEMO - PolicyConstants.LOW_WEMO)
                    + PolicyConstants.LOW_WEMO;
            Integer end_wemo = getEnergy(String.valueOf(start_wemo));

            RangeQuery rq = new RangeQuery(sTS, eTS, String.valueOf(start_wemo), String.valueOf(end_wemo), String.valueOf(start_temp),
                    String.valueOf(end_temp), String.valueOf(user.getUser_id()), infra.getName(), activity);

            rangeQueries.add(rq);
        }

        writer.writeJSONToFile(rangeQueries, PolicyConstants.RANGE_POLICY_1_DIR, null);

    }

    /**
     * Attributes are added to the policy based on a coin toss
     * @param numberOfPolicies
     */
    public void generateRangePolicy2(int numberOfPolicies) {

        List<RangeQuery> rangeQueries = new ArrayList<RangeQuery>();

        for (int i = 0; i < numberOfPolicies; i++) {
            int attrCount = (int) (r.nextGaussian() * 2 + 6); //mean -6, SD - 2
            if (attrCount <= 0 || attrCount > 9) attrCount = 6;
            RangeQuery rq = new RangeQuery();
            Field[] attributes = rq.getClass().getDeclaredFields();
            ArrayList<Field> attrList = new ArrayList<Field>();

            for (int j = 0; j < attrCount; j++) {
                Field attribute = attributes[r.nextInt(attributes.length)];
                if (attrList.contains(attribute)) {
                    j--;
                    continue;
                }
                if (attribute.getName().equalsIgnoreCase("user_id")) {
                    rq.setUser_id(String.valueOf(users.get(new Random().nextInt(users.size())).getUser_id()));
                } else if (attribute.getName().equalsIgnoreCase("location_id")) {
                    rq.setLocation_id(infras.get(new Random().nextInt(infras.size())).getName());
                } else if (attribute.getName().equalsIgnoreCase("start_timestamp")) {
                    rq.setStart_timestamp(getRandomTimeStamp());
                } else if (attribute.getName().equalsIgnoreCase("end_timestamp")) {
                    rq.setEnd_timestamp(getEndingTimeInterval(rq.getStart_timestamp()));
                } else if (attribute.getName().equalsIgnoreCase("start_wemo")) {
                    rq.setStart_wemo(String.valueOf(getEnergy(null)));
                } else if (attribute.getName().equalsIgnoreCase("end_wemo")) {
                    rq.setEnd_wemo(String.valueOf(getEnergy(rq.getStart_wemo())));
                } else if (attribute.getName().equalsIgnoreCase("activity")) {
                    rq.setActivity(PolicyConstants.ACTIVITIES.get(new Random().nextInt(PolicyConstants.ACTIVITIES.size())));
                } else if (attribute.getName().equalsIgnoreCase("start_temp")) {
                    rq.setStart_temp(String.valueOf(getTemperature(null)));
                } else if (attribute.getName().equalsIgnoreCase("end_temp")) {
                    rq.setEnd_temp(String.valueOf(getTemperature(rq.getStart_temp())));
                }
                attrList.add(attribute);
            }
            rangeQueries.add(rq);
        }
        writer.writeJSONToFile(rangeQueries, PolicyConstants.RANGE_POLICY_2_DIR, null);
    }

    /**
     * Only closed ranges are allowed e.g., if start_timestamp is chosen, then end_timestamp is included too.
     * @param numberOfPolicies
     */
    public void generateBEPolicy(int numberOfPolicies){
        List<BEPolicy> bePolicies = new ArrayList<>();

        for (int i = 0; i < numberOfPolicies; i++) {
            int attrCount = (int) (r.nextGaussian() * 2 + 6); //mean -6, SD - 2
            if (attrCount <= 0 || attrCount > 9) attrCount = 6;
            RangeQuery rq = new RangeQuery();
            Field[] attributes = rq.getClass().getDeclaredFields();
            ArrayList<Field> attrList = new ArrayList<Field>();

            for (int j = 0; j < attrCount; j++) {
                Field attribute = attributes[r.nextInt(attributes.length)];
                if (attrList.contains(attribute)) {
                    j--;
                    continue;
                }
                if (attribute.getName().equalsIgnoreCase("user_id")) {
                    rq.setUser_id(String.valueOf(users.get(new Random().nextInt(users.size())).getUser_id()));
                } else if (attribute.getName().equalsIgnoreCase("location_id")) {
                    rq.setLocation_id(infras.get(new Random().nextInt(infras.size())).getName());
                } else if (attribute.getName().equalsIgnoreCase("start_timestamp") ||
                        attribute.getName().equalsIgnoreCase("end_timestamp")) {
                    rq.setStart_timestamp(getRandomTimeStamp());
                    rq.setEnd_timestamp(getEndingTimeInterval(rq.getStart_timestamp()));
                } else if (attribute.getName().equalsIgnoreCase("start_wemo") ||
                        attribute.getName().equalsIgnoreCase("end_wemo")) {
                    rq.setStart_wemo(String.valueOf(getEnergy(null)));
                    rq.setEnd_wemo(String.valueOf(getEnergy(rq.getStart_wemo())));
                } else if (attribute.getName().equalsIgnoreCase("activity")) {
                    rq.setActivity(PolicyConstants.ACTIVITIES.get(new Random().nextInt(PolicyConstants.ACTIVITIES.size())));
                } else if (attribute.getName().equalsIgnoreCase("start_temp") ||
                        attribute.getName().equalsIgnoreCase("end_temp")) {
                    rq.setStart_temp(String.valueOf(getTemperature(null)));
                    rq.setEnd_temp(String.valueOf(getTemperature(rq.getStart_temp())));
                }
                attrList.add(attribute);
            }
            List<ObjectCondition> objectConditions = rq.createObjectCondition();
            bePolicies.add(new BEPolicy(String.valueOf(i), "Generated Policy " + i, objectConditions, PolicyConstants.DEFAULT_QC.asList(), "", ""));
        }
        writer.writeJSONToFile(bePolicies, PolicyConstants.BE_POLICY_DIR, null);
    }

}
