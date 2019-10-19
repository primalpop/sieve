package edu.uci.ics.tippers.generation.policy;

import edu.uci.ics.tippers.common.AttributeType;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.manager.PolicyPersistor;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;
import edu.uci.ics.tippers.model.policy.Operation;
import edu.uci.ics.tippers.model.policy.QuerierCondition;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

public class PolicyGen {

    /**
     * 1. Read all distinct user ids from database and store them in USER TABLE if it doesn't already exist
     * 2. Read all distinct locations from database and store them in LOCATION TABLE if it doesn't already exist
     * 3. Read smallest and largest values for start and end timestamps
     * 4. Choose random user, choose random location,
     * 4.1  choose random start timestamp between beg and fin, choose end based on start
     * 5. Check selectivity of the policy
     * 7. Check overlap and repetition factors
     * 8. Write it to the policy schema
     */

    private Connection connection;
    private Random r;
    private List<Integer> user_ids;
    private List<String> location_ids;
    public Timestamp start_beg, start_fin;
    public Timestamp end_beg, end_fin;

    public PolicyGen(){
       r = new Random();
       this.connection = MySQLConnectionManager.getInstance().getConnection();
       this.user_ids = getAllUsers();
       this.location_ids = getAllLocations();
       this.start_beg = getTimestamp(PolicyConstants.START_TIMESTAMP_ATTR, "MIN");
       this.start_fin = getTimestamp(PolicyConstants.START_TIMESTAMP_ATTR, "MAX");
       this.end_beg = getTimestamp(PolicyConstants.END_TIMESTAMP_ATTR, "MIN");
       this.end_fin = getTimestamp(PolicyConstants.END_TIMESTAMP_ATTR, "MAX");
    }


    private long getRandomTimeBetweenTwoDates (String colName) {
        if(colName.equalsIgnoreCase("start")){
            long diff = start_fin.getTime() - start_beg.getTime() + 1;
            return start_beg.getTime() + (long) (Math.random() * diff);
        }
        else{
            long diff = end_fin.getTime() - end_beg.getTime() + 1;
            return end_beg.getTime() + (long) (Math.random() * diff);
        }
    }

    public Timestamp getRandomTimeStamp(String colName) {
        LocalDateTime randomDate = Instant.ofEpochMilli(getRandomTimeBetweenTwoDates(colName)).atZone(ZoneId.systemDefault()).toLocalDateTime();
        return Timestamp.valueOf(randomDate);
    }

    public Timestamp getEndingTimeInterval(Timestamp start, List<Double> extensions){
        int hourIndex = new Random().nextInt(extensions.size());
        double rHour = extensions.get(hourIndex);
        long milliseconds = (long)(rHour * 60.0 * 60.0 * 1000.0);
        return new Timestamp(start.getTime() + milliseconds);
    }

    public Timestamp getEndingTimeWithExtension(Timestamp start, double hours){
        int hourIndex = new Random().nextInt(PolicyConstants.HOUR_EXTENSIONS.size());
        long milliseconds = (long)(hours * 60.0 * 60.0 * 1000.0);
        return new Timestamp(start.getTime() + milliseconds);
    }


    public List<Integer> getAllUsers() {
        PreparedStatement queryStm = null;
        user_ids = new ArrayList<>();
        try {
            queryStm = connection.prepareStatement("SELECT ID as id " +
                    "FROM USER");
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) user_ids.add(rs.getInt("id"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return user_ids;
    }

    public List<String> getAllLocations() {
        PreparedStatement queryStm = null;
        location_ids = new ArrayList<>();
        try {
            queryStm = connection.prepareStatement("SELECT NAME as room " +
                    "FROM LOCATION");
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) location_ids.add(rs.getString("room"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return location_ids;
    }

    public Timestamp getTimestamp(String colName, String valType){
        PreparedStatement queryStm = null;
        Timestamp ts = null;
        try{
            queryStm = connection.prepareStatement("SELECT " + valType + "(" + colName + ") AS value from PRESENCE");
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) ts = rs.getTimestamp("value");
        } catch(SQLException e) {
            e.printStackTrace();
        }
        return ts;
    }

    /**
     * Generates policies with all predicates of given list of attributes
     * @param policyID
     * @return
     */
    private List<ObjectCondition> generatePredicates(String policyID, List<String> attributes) {
        List<ObjectCondition> objectConditions = new ArrayList<>();
        int attrCount = (int) (r.nextGaussian() * 1 + 2); //mean - 3, SD - 1
        if (attrCount <= 1 || attrCount > attributes.size()) attrCount = 3;
        ArrayList<String> attrList = new ArrayList<>();
        while(attrCount - attrList.size() > 0) {
            String attribute = attributes.get(r.nextInt(attributes.size()));
            if (attrList.contains(attribute)) continue;
            attrList.add(attribute);
            SimpleDateFormat sdf = new SimpleDateFormat(PolicyConstants.TIMESTAMP_FORMAT);
            if (attribute.equalsIgnoreCase(PolicyConstants.START_TIMESTAMP_ATTR)) {
                Timestamp start_beg = getRandomTimeStamp(PolicyConstants.START_TIMESTAMP_ATTR);
                Timestamp start_fin = getEndingTimeInterval(start_beg, PolicyConstants.HOUR_EXTENSIONS);
                ObjectCondition tp_beg = new ObjectCondition(policyID, PolicyConstants.START_TIMESTAMP_ATTR, AttributeType.TIMESTAMP,
                        sdf.format(start_beg), Operation.GTE, sdf.format(start_fin), Operation.LTE);
                objectConditions.add(tp_beg);
            }
            if (attribute.equalsIgnoreCase(PolicyConstants.LOCATIONID_ATTR)) {
                ObjectCondition location = new ObjectCondition(policyID, PolicyConstants.LOCATIONID_ATTR, AttributeType.STRING,
                        location_ids.get(new Random().nextInt(location_ids.size())), Operation.EQ);
                objectConditions.add(location);
            }
            if (attribute.equalsIgnoreCase(PolicyConstants.END_TIMESTAMP_ATTR)) {
                Timestamp end_beg = getRandomTimeStamp(PolicyConstants.END_TIMESTAMP_ATTR);
                Timestamp end_fin = getEndingTimeInterval(end_beg, PolicyConstants.HOUR_EXTENSIONS);
                ObjectCondition tp_end = new ObjectCondition(policyID, PolicyConstants.END_TIMESTAMP_ATTR, AttributeType.TIMESTAMP,
                        sdf.format(end_beg), Operation.GTE, sdf.format(end_fin), Operation.LTE);
                objectConditions.add(tp_end);
            }
            if(attribute.equalsIgnoreCase(PolicyConstants.USERID_ATTR)){
                ObjectCondition owner = new ObjectCondition(policyID, PolicyConstants.USERID_ATTR, AttributeType.STRING,
                        String.valueOf(user_ids.get(new Random().nextInt(user_ids.size()))), Operation.EQ);
                objectConditions.add(owner);
            }
        }
        return objectConditions;
    }

    /**
     * #TODO: repetition not considered
     * Generates overlapping policies and stores them in the database
     * Policy Template: <id, querier, purpose, object_conditions, action, inserted_at>
     * id: random UUID
     * querier: same user always
     * owner: same as object_conditions.user_id
     * action: allow
     * purpose: analysis
     * inserted_at: current_timestamp
     * @param numberOfPolicies
     * @param threshold
     * @return
     */
    public void persistOverlappingPolicies(int numberOfPolicies, double threshold, boolean owner){
        List<BEPolicy> bePolicies = new ArrayList<>();
        boolean overlap = false;
        for (int i = 0; i < numberOfPolicies; i++) {
            String policyID =  UUID.randomUUID().toString();
            List<String> attributes = new ArrayList<>(PolicyConstants.REAL_ATTR_LIST);
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
                List<ObjectCondition> objectConditions = new ArrayList<>();
                if(owner){
                    ObjectCondition ownerPred = new ObjectCondition(policyID, PolicyConstants.USERID_ATTR, AttributeType.STRING,
                            String.valueOf(user_ids.get(new Random().nextInt(user_ids.size()))), Operation.EQ);
                    objectConditions.add(ownerPred);
                    attributes.remove(PolicyConstants.USERID_ATTR);
                }
                do {
                    objectConditions = generatePredicates(policyID, attributes);
                    selOfPolicy = BEPolicy.computeL(objectConditions);
                } while (!(selOfPolicy > 0.00000001 && selOfPolicy < 0.1));
                BEPolicy bePolicy = new BEPolicy(policyID,
                        objectConditions, querierConditions, "analysis",
                        "allow", new Timestamp(System.currentTimeMillis()));
                bePolicies.add(bePolicy);
            }
            double rand = Math.random();
            if (rand > threshold) overlap = true;
            else overlap = false;

        }
        PolicyPersistor polper = PolicyPersistor.getInstance();
        for (BEPolicy bePolicy: bePolicies) {
            polper.insertPolicy(bePolicy);
        }
    }

    /**
     * //TODO: add a progressive selectivity check
     * @param querier
     * @param owner_id
     * @return
     */
    public BEPolicy createPolicy(int querier, int owner_id, String role) {
        String policyID = UUID.randomUUID().toString();
        List<String> attributes = new ArrayList<>(PolicyConstants.REAL_ATTR_LIST);
        List<QuerierCondition> querierConditions = new ArrayList<>(Arrays.asList(
                new QuerierCondition(policyID, "policy_type", AttributeType.STRING, Operation.EQ, "user"),
                new QuerierCondition(policyID, "querier", AttributeType.STRING, Operation.EQ, String.valueOf(querier))));

        double selOfPolicy = 0.0;
        List<ObjectCondition> objectConditions = new ArrayList<>();
        ObjectCondition ownerPred = new ObjectCondition(policyID, PolicyConstants.USERID_ATTR, AttributeType.STRING,
                String.valueOf(owner_id), Operation.EQ);
        objectConditions.add(ownerPred);
        attributes.remove(PolicyConstants.USERID_ATTR);
        objectConditions.addAll(generateSemiRealistic(policyID, attributes, role));
        return new BEPolicy(policyID,
                objectConditions, querierConditions, "analysis",
                "allow", new Timestamp(System.currentTimeMillis()));

    }



    /**
     * Generates semi-realistic policies with all predicates of given list of attributes
     * @param policyID
     * @return
     */
    private List<ObjectCondition> generateSemiRealistic(String policyID, List<String> attributes, String role) {
        List<ObjectCondition> objectConditions = new ArrayList<>();
        int attrCount = (int) (r.nextGaussian() * 1 + 2); //mean - 3, SD - 1
        if (attrCount <= 1 || attrCount > attributes.size()) attrCount = 3;
        ArrayList<String> attrList = new ArrayList<>();
        while(attrCount - attrList.size() > 0) {
            String attribute = attributes.get(r.nextInt(attributes.size()));
            if (attrList.contains(attribute)) continue;
            attrList.add(attribute);
            SimpleDateFormat sdf = new SimpleDateFormat(PolicyConstants.TIMESTAMP_FORMAT);
            if (attribute.equalsIgnoreCase(PolicyConstants.START_TIMESTAMP_ATTR)) {
                Timestamp start_beg = getRandomTimeStamp(PolicyConstants.START_TIMESTAMP_ATTR);
                Timestamp start_fin = getEndingTimeInterval(start_beg, PolicyConstants.HOUR_EXTENSIONS);
                ObjectCondition tp_beg = new ObjectCondition(policyID, PolicyConstants.START_TIMESTAMP_ATTR, AttributeType.TIMESTAMP,
                        sdf.format(start_beg), Operation.GTE, sdf.format(start_fin), Operation.LTE);
                objectConditions.add(tp_beg);
            }
            if (attribute.equalsIgnoreCase(PolicyConstants.LOCATIONID_ATTR)) {
                ObjectCondition location = new ObjectCondition(policyID, PolicyConstants.LOCATIONID_ATTR, AttributeType.STRING,
                        location_ids.get(new Random().nextInt(location_ids.size())), Operation.EQ);
                objectConditions.add(location);
            }
            if (attribute.equalsIgnoreCase(PolicyConstants.END_TIMESTAMP_ATTR)) {
                Timestamp end_beg = getRandomTimeStamp(PolicyConstants.END_TIMESTAMP_ATTR);
                Timestamp end_fin = getEndingTimeInterval(end_beg, PolicyConstants.HOUR_EXTENSIONS);
                ObjectCondition tp_end = new ObjectCondition(policyID, PolicyConstants.END_TIMESTAMP_ATTR, AttributeType.TIMESTAMP,
                        sdf.format(end_beg), Operation.GTE, sdf.format(end_fin), Operation.LTE);
                objectConditions.add(tp_end);
            }
        }
        return objectConditions;
    }



    public static void main(String [] args){
        PolicyGen pg = new PolicyGen();
        pg.persistOverlappingPolicies(5, 0.3, true);
    }
}
