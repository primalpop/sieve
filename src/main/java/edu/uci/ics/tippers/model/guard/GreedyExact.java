package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;
import jdk.nashorn.internal.runtime.JSONFunctions;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class GreedyExact {

    //Original expression
    BEExpression expression;

    // Chosen multiple
    List<ObjectCondition> multiplier;

    // Polices from the original expression that contain the multiple
    GreedyExact quotient;

    // Polices from the original expression that does not contain the multiple
    GreedyExact reminder;

    //Cost of evaluating the expression
    Long cost;

    MySQLQueryManager mySQLQueryManager = new MySQLQueryManager();

    Map<String, List<Bucket>> bucketMap;

    private java.sql.Connection conn;

    public GreedyExact() {
        this.expression = new BEExpression();
        this.multiplier = new ArrayList<ObjectCondition>();
        this.cost = -1L;
        this.conn = MySQLConnectionManager.getInstance().getConnection();
        fillBuckets();
    }

    public GreedyExact(BEExpression beExpression) {
        this.expression = new BEExpression(beExpression);
        this.multiplier = new ArrayList<ObjectCondition>();
        this.cost = -1L;
        this.conn = MySQLConnectionManager.getInstance().getConnection();
        fillBuckets();
    }


    public BEExpression getExpression() {
        return expression;
    }

    public void setExpression(BEExpression expression) {
        this.expression = expression;
    }

    public List<ObjectCondition> getMultiplier() {
        return multiplier;
    }

    public void setMultiplier(List<ObjectCondition> multiplier) {
        this.multiplier = multiplier;
    }

    public GreedyExact getQuotient() {
        return quotient;
    }

    public void setQuotient(GreedyExact quotient) {
        this.quotient = quotient;
    }

    public GreedyExact getReminder() {
        return reminder;
    }

    public void setReminder(GreedyExact reminder) {
        this.reminder = reminder;
    }

    public Long getCost() {
        return cost;
    }

    public void setCost(Long cost) {
        this.cost = cost;
    }

    public static Calendar timestampStrToCal(String timestamp) {
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat(PolicyConstants.TIMESTAMP_FORMAT);
        try {
            cal.setTime(sdf.parse(timestamp));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return cal;
    }

    public void fillBuckets(){
        bucketMap = new HashMap<>();
        this.bucketMap.put(PolicyConstants.USERID_ATTR, getHistogram("user_id", "String", "equiheight"));
        this.bucketMap.put(PolicyConstants.TIMESTAMP_ATTR, getHistogram("timeStamp", "DateTime", "equiheight"));
        this.bucketMap.put(PolicyConstants.LOCATIONID_ATTR, getHistogram("location_id", "String", "singleton"));
        this.bucketMap.put(PolicyConstants.ENERGY_ATTR, getHistogram("energy", "String", "singleton"));
        this.bucketMap.put(PolicyConstants.TEMPERATURE_ATTR, getHistogram("temperature", "String", "singleton"));
        this.bucketMap.put(PolicyConstants.ACTIVITY_ATTR, getHistogram("activity", "String", "singleton"));
    }

    public List<Bucket> getHistogram(String attribute, String attribute_type, String histogram_type) {
        List<Bucket> hBuckets = new ArrayList<>();
        PreparedStatement ps = null;
        if (attribute_type.equalsIgnoreCase("String") && histogram_type.equalsIgnoreCase("singleton")) {
            try {
                ps = conn.prepareStatement(
                        "SELECT FROM_BASE64(SUBSTRING_INDEX(v, ':', -1)) value, concat(round(c*100,1),'%') cumulfreq, " +
                                "CONCAT(round((c - LAG(c, 1, 0) over()) * 100,1), '%') freq " +
                                "FROM information_schema.column_statistics, JSON_TABLE(histogram->'$.buckets', " +
                                "'$[*]' COLUMNS(v VARCHAR(60) PATH '$[0]', c double PATH '$[1]')) hist  " +
                                "where column_name = ?;");

                ps.setString(1, attribute);
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    Bucket bucket = new Bucket();
                    bucket.setValue(rs.getString("value"));
                    bucket.setCumulfreq(Double.parseDouble(rs.getString("cumulfreq").replaceAll("[^\\d.]", "")));
                    bucket.setFreq(Double.parseDouble(rs.getString("freq").replaceAll("[^\\d.]", "")));
                    hBuckets.add(bucket);
                }
                rs.close();
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else if (attribute_type.equalsIgnoreCase("String") && histogram_type.equalsIgnoreCase("equiheight")) {
            try {
                ps = conn.prepareStatement("SELECT FROM_BASE64(SUBSTRING_INDEX(v1, ':', -1)) lvalue, " +
                        "FROM_BASE64(SUBSTRING_INDEX(v2, ':', -1)) uvalue, concat(round(c*100,1),'%') cumulfreq, " +
                        "CONCAT(round((c - LAG(c, 1, 0) over()) * 100,1), '%') freq, numItems FROM information_schema.column_statistics, " +
                        "JSON_TABLE(histogram->'$.buckets',       '$[*]' COLUMNS(v1 VARCHAR(60) PATH '$[0]', v2 VARCHAR(60) PATH '$[1]'," +
                        " c double PATH '$[2]', numItems integer PATH '$[3]')) hist  where column_name = ?;");
                ps.setString(1, attribute);
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    Bucket bucket = new Bucket();
                    bucket.setLower(rs.getString("lvalue"));
                    bucket.setUpper(rs.getString("uvalue"));
                    bucket.setCumulfreq(Double.parseDouble(rs.getString("cumulfreq").replaceAll("[^\\d.]", "")));
                    bucket.setFreq(Double.parseDouble(rs.getString("freq").replaceAll("[^\\d.]", "")));
                    bucket.setNumberOfItems(rs.getInt("numItems"));
                    hBuckets.add(bucket);
                }
                rs.close();
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else if (attribute_type.equalsIgnoreCase("DateTime") && histogram_type.equalsIgnoreCase("equiheight")) {
            try {
                ps = conn.prepareStatement("SELECT lvalue, uvalue, concat(round(c*100,1),'%') cumulfreq, " +
                        "CONCAT(round((c - LAG(c, 1, 0) over()) * 100,1), '%') freq, numItems " +
                        "FROM information_schema.column_statistics, JSON_TABLE(histogram->'$.buckets',       " +
                        "'$[*]' COLUMNS(lvalue VARCHAR(60) PATH '$[0]', uvalue VARCHAR(60) PATH '$[1]', c double PATH '$[2]', " +
                        "numItems integer PATH '$[3]')) hist  where column_name = ?;");
                ps.setString(1, attribute);
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    Bucket bucket = new Bucket();
                    bucket.setLower(rs.getString("lvalue"));
                    bucket.setUpper(rs.getString("uvalue"));
                    bucket.setCumulfreq(Double.parseDouble(rs.getString("cumulfreq").replaceAll("[^\\d.]", "")));
                    bucket.setFreq(Double.parseDouble(rs.getString("freq").replaceAll("[^\\d.]", "")));
                    bucket.setNumberOfItems(rs.getInt("numItems"));
                    hBuckets.add(bucket);
                }
                rs.close();
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else {
            throw new PolicyEngineException("Unknown Histogram type");
        }
        return hBuckets;
    }

    private List<Bucket> inBuckets(ObjectCondition objectCondition){
        List mBuckets = new ArrayList<Bucket>();

        mBuckets = bucketMap.get(objectCondition.getAttribute()).stream()
                .filter(b -> Integer.parseInt(b.getValue()) >
                        Integer.parseInt(objectCondition.getBooleanPredicates().get(0).getValue())
                        && Integer.parseInt(b.getValue()) <
                        Integer.parseInt(objectCondition.getBooleanPredicates().get(1).getValue()))
                .collect(Collectors.toList());

        return mBuckets;
    }


    public double computeL(ObjectCondition objectCondition){
        List mBuckets = null;
        double selectivity = 0.0001;
        if(objectCondition.getAttribute().equalsIgnoreCase(PolicyConstants.TEMPERATURE_ATTR) ||
                objectCondition.getAttribute().equalsIgnoreCase(PolicyConstants.ENERGY_ATTR)){
            selectivity += bucketMap.get(objectCondition.getAttribute()).stream()
                    .filter(b -> Integer.parseInt(b.getValue()) >=
                            Integer.parseInt(objectCondition.getBooleanPredicates().get(0).getValue())
                            && Integer.parseInt(b.getValue()) <=
                            Integer.parseInt(objectCondition.getBooleanPredicates().get(1).getValue()))
                    .mapToDouble(b -> b.getFreq())
                    .sum();
        }
        else if (objectCondition.getAttribute().equalsIgnoreCase(PolicyConstants.LOCATIONID_ATTR) ||
                objectCondition.getAttribute().equalsIgnoreCase(PolicyConstants.ACTIVITY_ATTR)){
            Bucket bucket = bucketMap.get(objectCondition.getAttribute()).stream()
                    .filter(b -> b.getValue().equalsIgnoreCase(objectCondition.getBooleanPredicates().get(0).getValue()))
                    .findFirst()
                    .orElse(null);
            selectivity += bucket.getFreq();

        }
        else if (objectCondition.getAttribute().equalsIgnoreCase(PolicyConstants.USERID_ATTR)){
            Bucket bucket = bucketMap.get(objectCondition.getAttribute()).stream()
                    .filter(b -> Integer.parseInt(b.getLower()) >=
                            Integer.parseInt(objectCondition.getBooleanPredicates().get(0).getValue())
                            && Integer.parseInt(b.getUpper()) <=
                            Integer.parseInt(objectCondition.getBooleanPredicates().get(1).getValue()))
                    .findFirst()
                    .orElse(null);
            selectivity += bucket.getFreq()/bucket.getNumberOfItems();

        }
        else if (objectCondition.getAttribute().equalsIgnoreCase(PolicyConstants.TIMESTAMP_ATTR)){
            //TODO: Overestimates the selectivity as the partially contained buckets are completely counted
            selectivity += bucketMap.get(objectCondition.getAttribute()).stream()
                    .filter(b -> timestampStrToCal(b.getLower())
                            .compareTo(timestampStrToCal(objectCondition.getBooleanPredicates().get(0).getValue())) < 0
                            && timestampStrToCal(b.getUpper())
                            .compareTo(timestampStrToCal(objectCondition.getBooleanPredicates().get(1).getValue())) > 0)
                    .mapToDouble(b -> b.getFreq())
                    .sum();
        }
        else {
            throw new PolicyEngineException("Unknown attribute");
        }
        return selectivity;
    }

    /**
     * Selectivity of a conjunctive expression
     * e.g., A = u and B = v
     * sel = set (A) * sel (B)
     * @param objectConditions
     * @return
     */
    public double computeL(Collection<ObjectCondition> objectConditions){
        double selectivity = 0.0001;
        for (ObjectCondition obj: objectConditions) {
            selectivity *= computeL(obj);
        }
        return selectivity;
    }

    /**
     * Selectivity of a disjunctive expression
     * e.g., A = u or B = v
     * sel = 1 - ((1 - sel(A)) * (1 - sel (B)))
     * @param beExpression
     * @return
     */
    public double computeL(BEExpression beExpression){
        double selectivity = 0.0001;
        for (BEPolicy bePolicy: beExpression.getPolicies()) {
            selectivity *= (1 - computeL(bePolicy.getObject_conditions()));
        }
        return 1 - selectivity;
    }


    //TODO: DEBUG ITT!!!
    public long computeGain(BEExpression original, Set<ObjectCondition> objSet, BEExpression quotient) {
        long gain = (long) ((quotient.getPolicies().size() - 1)* computeL(objSet) * PolicyConstants.NUMBER_OR_TUPLES);
        for (BEPolicy bePolicy: original.getPolicies()) {
            gain += (computeL(bePolicy.getObject_conditions()) * PolicyConstants.NUMBER_OR_TUPLES);
        }
        gain -= computeL(quotient) * PolicyConstants.NUMBER_OR_TUPLES;
        return gain;
    }

    public void GFactorize() {
        if (this.expression.getPolicies().isEmpty()) return;
        Set<Set<ObjectCondition>> powerSet = new HashSet<Set<ObjectCondition>>();
        for (int i = 0; i < this.expression.getPolicies().size(); i++) {
            BEPolicy bp = this.expression.getPolicies().get(i);
            Set<Set<ObjectCondition>> subPowerSet = bp.calculatePowerSet();
            powerSet.addAll(subPowerSet);
            subPowerSet = null; //forcing garbage collection
        }
        GreedyExact currentFactor = new GreedyExact(this.expression);
        for (Set<ObjectCondition> objSet : powerSet) {
            if (objSet.size() == 0) continue;
            BEExpression temp = new BEExpression(this.expression);
            temp.checkAgainstPolices(objSet);
            if (temp.getPolicies().size() > 1) { //was able to factorize
                currentFactor.multiplier = new ArrayList<>(objSet);
                currentFactor.quotient = new GreedyExact(temp);
                currentFactor.quotient.expression.removeFromPolicies(objSet);
                currentFactor.reminder = new GreedyExact(this.expression);
                currentFactor.reminder.expression.getPolicies().removeAll(temp.getPolicies());
                currentFactor.cost = computeGain(this.expression, objSet, temp);
                if (this.cost < currentFactor.cost) {
                    this.multiplier = currentFactor.getMultiplier();
                    this.quotient = currentFactor.getQuotient();
                    this.reminder = currentFactor.getReminder();
//                    this.reminder.GFactorize();
                }
                currentFactor = new GreedyExact(this.expression);
            }
        }
    }


    public String createQueryFromExactFactor() {
        if (multiplier.isEmpty()) {
            if (expression != null) {
                return this.expression.createQueryFromPolices();
            } else
                return "";
        }
        StringBuilder query = new StringBuilder();
        for (ObjectCondition mul : multiplier) {
            query.append(mul.print());
            query.append(PolicyConstants.CONJUNCTION);
        }
        query.append("(");
        query.append(this.quotient.createQueryFromExactFactor());
        query.append(")");
        if (!this.reminder.expression.getPolicies().isEmpty()) {
            query.append(PolicyConstants.DISJUNCTION);
            query.append("(");
            query.append(this.reminder.createQueryFromExactFactor());
            query.append(")");
        }
        return query.toString();
    }


}
