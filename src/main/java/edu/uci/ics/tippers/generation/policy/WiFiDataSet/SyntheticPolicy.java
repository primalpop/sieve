package edu.uci.ics.tippers.generation.policy.WiFiDataSet;

import edu.uci.ics.tippers.common.AttributeType;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.manager.FlatPolicyPersistor;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;
import edu.uci.ics.tippers.model.policy.Operation;
import edu.uci.ics.tippers.model.policy.QuerierCondition;
import edu.uci.ics.tippers.model.query.RangeQuery;
import edu.uci.ics.tippers.model.tippers.dontuse.Infrastructure;
import edu.uci.ics.tippers.model.tippers.dontuse.User;
import edu.uci.ics.tippers.generation.data.DataGeneration;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

/**
 * Author primpap
 */


public class SyntheticPolicy {

    List<Infrastructure> infras;
    List<User> users;
    Random r;
    Writer writer;
    MySQLQueryManager mySQLQueryManager = new MySQLQueryManager();

    public SyntheticPolicy() {

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
            if (energy + noise > energy + 10) return energy + 10;
            return energy + noise;
        }
        else
            return energy + 1;
    }

    private List<ObjectCondition> generatePredicates(String policyID, List<String> attributes){
        RangeQuery rq = new RangeQuery();
        int attrCount = (int) (r.nextGaussian() * 2 + 4); //mean - 4, SD - 2
        if (attrCount <= 1 || attrCount > attributes.size()) attrCount = 4;
        ArrayList<String> attrList = new ArrayList<>();
        if(attrCount >=3) {
            rq.setStart_timestamp(getRandomTimeStamp());
            rq.setEnd_timestamp(getEndingTimeInterval(rq.getStart_timestamp()));
            attrList.add(PolicyConstants.START_DATE);
        }
        while(attrCount - attrList.size() > 0) {
            String attribute = attributes.get(r.nextInt(attributes.size()));
            if (attrList.contains(attribute)) continue;
            if (attribute.equalsIgnoreCase(PolicyConstants.USERID_ATTR)) {
                rq.setUser_id(String.valueOf(users.get(new Random().nextInt(users.size())).getUser_id()));
            }
            if (attribute.equalsIgnoreCase(PolicyConstants.LOCATIONID_ATTR)) {
                rq.setLocation_id(infras.get(new Random().nextInt(infras.size())).getName());
            } else if (attribute.equalsIgnoreCase(PolicyConstants.ENERGY_ATTR)){
                rq.setStart_wemo(String.valueOf(getEnergy(null)));
                rq.setEnd_wemo(String.valueOf(getEnergy(rq.getStart_wemo())));
            } else if (attribute.equalsIgnoreCase(PolicyConstants.ACTIVITY_ATTR)) {
                rq.setActivity(PolicyConstants.ACTIVITIES.get(new Random().nextInt(PolicyConstants.ACTIVITIES.size())));
            } else if (attribute.equalsIgnoreCase(PolicyConstants.TEMPERATURE_ATTR)){
                rq.setStart_temp(String.valueOf(getTemperature(null)));
                rq.setEnd_temp(String.valueOf(getTemperature(rq.getStart_temp())));
            }
            else if (attribute.equalsIgnoreCase(PolicyConstants.START_DATE)) {
                rq.setStart_timestamp(getRandomTimeStamp());
                rq.setEnd_timestamp(getEndingTimeInterval(rq.getStart_timestamp()));
            }
            attrList.add(attribute);
        }
        return rq.createObjectCondition(policyID);
    }

    /**
     * Only closed ranges are allowed e.g., if start_timestamp is chosen, then end_timestamp is included too.
     * @param numberOfPolicies
     */
    public void generateBEPolicy(int numberOfPolicies, List<String> attributes){
        List<BEPolicy> bePolicies = new ArrayList<>();

        for (int i = 0; i < numberOfPolicies; i++) {
            List<ObjectCondition> objectConditions;
            objectConditions = generatePredicates(String.valueOf(i), attributes);
            BEPolicy bePolicy = new BEPolicy(String.valueOf(i), "Generated Policy " + i , objectConditions, PolicyConstants.DEFAULT_QC.asList(), "", "");
            bePolicies.add(bePolicy);
        }
        writer.writeJSONToFile(bePolicies, PolicyConstants.BE_POLICY_DIR, null);
    }

    /**
     * Generates overlapping policies without checking selectivity
     * @param numberOfPolicies
     * @param threshold
     * @param attributes
     * @param previous
     * @return
     */
    public List<BEPolicy> generateOverlappingPolicies(int numberOfPolicies, double threshold, List<String> attributes, List<BEPolicy> previous){
        List<BEPolicy> bePolicies = new ArrayList<>();
        bePolicies.addAll(previous);
        boolean overlap = false;
        for (int i = previous.size(); i < numberOfPolicies; i++) {
            String policyID =  UUID.randomUUID().toString();
            List<QuerierCondition> querierConditions = new ArrayList<>(Arrays.asList(
                    new QuerierCondition(policyID, "policy_type", AttributeType.STRING, Operation.EQ,"user"),
                    new QuerierCondition(policyID, "querier", AttributeType.STRING, Operation.EQ, "10")));
            if (overlap) {
                BEPolicy oPolicy = new BEPolicy(bePolicies.get(new Random().nextInt(i)));
                oPolicy.setId(policyID);
                oPolicy.setQuerier_conditions(querierConditions);
                for (ObjectCondition objC: oPolicy.getObject_conditions()) {
                    objC.setPolicy_id(policyID);
                    objC.shift();
                }
                bePolicies.add(oPolicy);
            }
            else {
                List<ObjectCondition> objectConditions;
                objectConditions = generatePredicates(policyID, attributes);
                BEPolicy bePolicy = new BEPolicy(policyID,
                        objectConditions, querierConditions, "analysis",
                        "allow", new Timestamp(System.currentTimeMillis()));
                bePolicies.add(bePolicy);
            }
            double rand = Math.random();
            if (rand > threshold) overlap = true;
            else overlap = false;

        }
        writer.writeJSONToFile(bePolicies, PolicyConstants.BE_POLICY_DIR, null);
        return bePolicies;
    }

    /**
     * Generates policies with user id predicates (which is the owner attribute of policy)
     * @param policyID
     * @param attributes
     * @return
     */
    private List<ObjectCondition> generateOwnerPolicies(String policyID, List<String> attributes){
        RangeQuery rq = new RangeQuery();
        int attrCount = (int) (r.nextGaussian() * 2 + 4); //mean - 4, SD - 2
        if (attrCount <= 1 || attrCount > attributes.size()) attrCount = 4;
        ArrayList<String> attrList = new ArrayList<>();
        rq.setUser_id(String.valueOf(users.get(new Random().nextInt(users.size())).getUser_id()));
        attrList.add(PolicyConstants.USERID_ATTR);
        double rand = Math.random();
        double TIMESTAMP_INCLUDE = 0.1;
        if (rand > TIMESTAMP_INCLUDE) {
            rq.setStart_timestamp(getRandomTimeStamp());
            rq.setEnd_timestamp(getEndingTimeInterval(rq.getStart_timestamp()));
            attrList.add(PolicyConstants.START_DATE);
        }
        while(attrCount - attrList.size() > 0) {
            String attribute = attributes.get(r.nextInt(attributes.size()));
            if (attrList.contains(attribute)) continue;
            if (attribute.equalsIgnoreCase(PolicyConstants.LOCATIONID_ATTR)) {
                rq.setLocation_id(infras.get(new Random().nextInt(infras.size())).getName());
            } else if (attribute.equalsIgnoreCase(PolicyConstants.ENERGY_ATTR)){
                rq.setStart_wemo(String.valueOf(getEnergy(null)));
                rq.setEnd_wemo(String.valueOf(getEnergy(rq.getStart_wemo())));
            } else if (attribute.equalsIgnoreCase(PolicyConstants.ACTIVITY_ATTR)) {
                rq.setActivity(PolicyConstants.ACTIVITIES.get(new Random().nextInt(PolicyConstants.ACTIVITIES.size())));
            } else if (attribute.equalsIgnoreCase(PolicyConstants.TEMPERATURE_ATTR)){
                rq.setStart_temp(String.valueOf(getTemperature(null)));
                rq.setEnd_temp(String.valueOf(getTemperature(rq.getStart_temp())));
            }
            attrList.add(attribute);
        }
        return rq.createObjectCondition(policyID);
    }


    /**
     * Generates overlapping policies and stores them in the database
     * Policy Template: <id, querier, owner, purpose, object_conditions, action, inserted_at>
     * id: random UUID
     * querier: same user always
     * owner: same as object_conditions.user_id
     * action: allow
     * purpose: analysis
     * inserted_at: current_timestamp
     * @param numberOfPolicies
     * @param threshold
     * @param attributes
     * @return
     */
    public void persistOverlappingPolicies(int numberOfPolicies, double threshold, List<String> attributes){
        FlatPolicyPersistor sp = FlatPolicyPersistor.getInstance();
        List<BEPolicy> bePolicies = new ArrayList<>();
        boolean overlap = false;
        for (int i = 0; i < numberOfPolicies; i++) {
            String policyID =  UUID.randomUUID().toString();
            List<QuerierCondition> querierConditions = new ArrayList<>(Arrays.asList(
                    new QuerierCondition(policyID, "policy_type", AttributeType.STRING, Operation.EQ,"user"),
                    new QuerierCondition(policyID, "querier", AttributeType.STRING, Operation.EQ, "10")));
            if (overlap) {
                BEPolicy oPolicy = new BEPolicy(bePolicies.get(new Random().nextInt(i)));
                oPolicy.setId(policyID);
                oPolicy.setQuerier_conditions(querierConditions);
                oPolicy.setAction("allow");
                oPolicy.setInserted_at(new Timestamp(System.currentTimeMillis()));
                oPolicy.setPurpose("analysis");
                for (ObjectCondition objC: oPolicy.getObject_conditions()) {
                    objC.setPolicy_id(String.valueOf(i));
                    objC.shift();
                }
                bePolicies.add(oPolicy);
            }
            else {
                double selOfPolicy = 0.0;
                List<ObjectCondition> objectConditions;
                BEPolicy bePolicy = null;
                do {
                    objectConditions = generateOwnerPolicies(policyID, attributes);
                    bePolicy = new BEPolicy(policyID,
                            objectConditions, querierConditions, "analysis",
                            "allow", new Timestamp(System.currentTimeMillis()));
                    selOfPolicy = bePolicy.computeL();
                } while (!(selOfPolicy > 0.00000001 && selOfPolicy < 0.1));
                bePolicies.add(bePolicy);
            }
            double rand = Math.random();
            if (rand > threshold) overlap = true;
            else overlap = false;

        }
        sp.insertPolicies(bePolicies);
    }

    /**
     * Generate semi-realistic policies
     * Only based on user_id, location and timestamp attributes
     * @param numberOfPolicies
     * @param attributes
     * @param previous
     * @return
     */
    public List<BEPolicy> generateSemiRealisticPolicies(int numberOfPolicies, List<String> attributes, List<BEPolicy> previous){
        //TODO: DEBUG THIS
        //Creating Location Profiles
        ArrayList<String> infra1 = new ArrayList<>();
        ArrayList<String> infra2 = new ArrayList<>();
        ArrayList<String> infra3 = new ArrayList<>();
        ArrayList<ArrayList<String>> infs = new ArrayList<ArrayList<String>>();
        infs.add(infra1);
        infs.add(infra2);
        infs.add(infra3);
        int Max = 3;
        int Min = 1;
        for (Infrastructure inf: infras) {
            int c = Min + (int)(Math.random() * ((Max - Min) + 1));
            infs.get(c).add(inf.getName());
        }
        //Creating Time Profiles
        ArrayList<Integer> time1 = new ArrayList<>();
        ArrayList<Integer> time2 = new ArrayList<>();
        ArrayList<Integer> time3 = new ArrayList<>();
        time1.add(9);
        time1.add(17);
        time2.add(10);
        time2.add(18);
        time3.add(11);
        time3.add(19);
        ArrayList<ArrayList<Integer>> times = new ArrayList<ArrayList<Integer>>();
        times.add(time1);
        times.add(time2);
        times.add(time3);

        //adding previous policies
        List<BEPolicy> bePolicies = new ArrayList<>();
        bePolicies.addAll(previous);

        for (int i = previous.size(); i < numberOfPolicies; i++) {
            RangeQuery rq = new RangeQuery();
            int attrCount = (int) (r.nextGaussian() * 1 + 2); //mean - 2, SD - 1
            if (attrCount <= 1 || attrCount > attributes.size()) attrCount = 2;
            ArrayList<String> attrList = new ArrayList<>();
            for (int j = 1; j < attrCount; j++) {
                String attribute = attributes.get(r.nextInt(attributes.size()));
                if (attrList.contains(attribute)) {
                    j--;
                    continue;
                }
                if (attribute.equalsIgnoreCase(PolicyConstants.USERID_ATTR)) {
                    rq.setUser_id(String.valueOf(users.get(new Random().nextInt(users.size())).getUser_id()));
                }
                if (attribute.equalsIgnoreCase(PolicyConstants.LOCATIONID_ATTR)) {
                    int locProf = Min + (int)(Math.random() * ((Max - Min) + 1));
                    rq.setLocation_id(infs.get(locProf).get(new Random().nextInt(infs.get(locProf).size())));
                }
                if (attribute.equalsIgnoreCase(PolicyConstants.START_DATE)) {
                    rq.setStart_timestamp(getRandomTimeStamp());
                    rq.setEnd_timestamp(getEndingTimeInterval(rq.getStart_timestamp()));
                    Calendar start = Calendar.getInstance();
                    start.setTimeInMillis(rq.getStart_timestamp().getTime());
                    Calendar end = Calendar.getInstance();
                    end.setTimeInMillis(rq.getEnd_timestamp().getTime());
                    int tProf = Min + (int)(Math.random() * ((Max - Min) + 1));
                    start.set(Calendar.HOUR_OF_DAY, times.get(tProf).get(0));
                    end.set(Calendar.HOUR_OF_DAY, times.get(tProf).get(1));
                }
                attrList.add(attribute);
            }
            List<ObjectCondition> objectConditions = rq.createObjectCondition(String.valueOf(i));
            BEPolicy bePolicy = new BEPolicy(String.valueOf(i), "Generated Policy " + i, objectConditions, PolicyConstants.DEFAULT_QC.asList(), "", "");
            bePolicies.add(bePolicy);
        }
        writer.writeJSONToFile(bePolicies, PolicyConstants.BE_POLICY_DIR, null);
        return bePolicies;
    }


}