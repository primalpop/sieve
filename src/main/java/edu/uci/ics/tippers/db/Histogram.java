package edu.uci.ics.tippers.db;

import com.fasterxml.jackson.core.type.TypeReference;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import edu.uci.ics.tippers.fileop.Reader;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.model.guard.Bucket;

import java.io.*;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class Histogram {

    private static java.sql.Connection conn = MySQLConnectionManager.getInstance().getConnection();

    private static Writer writer = new Writer();

    private static Map<String, List<Bucket>> bucketMap;

    private static Histogram _instance;

    private Histogram(List<String> attribute_names) {
        File histDir = new File(PolicyConstants.HISTOGRAM_DIR);
        if (histDir.isDirectory() && Objects.requireNonNull(histDir.list()).length == 0)
            writeBuckets();
        else retrieveBuckets(attribute_names);
    }

    public static Histogram getInstance() {
        //TODO: Pass this as an argument from whereever it is called
        List<String> attribute_names = PolicyConstants.TPCH_ORDERS_ATTR_LIST;
        if (_instance == null)
            _instance = new Histogram(attribute_names);
        return _instance;
    }

    private static List<Bucket> getHistogram(String attribute, String attribute_type, String histogram_type) {
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
                    bucket.setAttribute(attribute);
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
                    bucket.setAttribute(attribute);
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
                    bucket.setAttribute(attribute);
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
        } else if (attribute_type.equalsIgnoreCase("Integer") && histogram_type.equalsIgnoreCase("equiheight")) {
            try {
                ps = conn.prepareStatement("SELECT lvalue, uvalue, concat(round(c*100,1),'%') cumulfreq, " +
                        "CONCAT(round((c - LAG(c, 1, 0) over()) * 100,1), '%') freq, numItems " +
                        "FROM information_schema.column_statistics, JSON_TABLE(histogram->'$.buckets',       " +
                        "'$[*]' COLUMNS(lvalue int PATH '$[0]', uvalue int PATH '$[1]', c double PATH '$[2]', " +
                        "numItems integer PATH '$[3]')) hist  where column_name = ? and lvalue is NOT NULL;");
                ps.setString(1, attribute);
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    Bucket bucket = new Bucket();
                    bucket.setAttribute(attribute);
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
        } else if(attribute_type.equalsIgnoreCase("Date") && histogram_type.equalsIgnoreCase("singleton")){
            try {
                ps = conn.prepareStatement("SELECT value, concat(round(c*100,1),'%') cumulfreq, " +
                        "CONCAT(round((c - LAG(c, 1, 0) over()) * 100,1), '%') freq, numItems " +
                        "FROM information_schema.column_statistics, JSON_TABLE(histogram->'$.buckets',       " +
                        "'$[*]' COLUMNS(value date PATH '$[0]', c double PATH '$[1]', " +
                        "numItems integer PATH '$[2]')) hist  where column_name = ?");
                ps.setString(1, attribute);
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    Bucket bucket = new Bucket();
                    bucket.setAttribute(attribute);
                    bucket.setValue(rs.getString("value"));
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
        } else if(attribute_type.equalsIgnoreCase("Time") && histogram_type.equalsIgnoreCase("equiheight")){
            try {
                ps = conn.prepareStatement("SELECT lvalue, uvalue, concat(round(c*100,1),'%') cumulfreq, " +
                        "CONCAT(round((c - LAG(c, 1, 0) over()) * 100,1), '%') freq, numItems " +
                        "FROM information_schema.column_statistics, JSON_TABLE(histogram->'$.buckets',       " +
                        "'$[*]' COLUMNS(lvalue time PATH '$[0]', uvalue time PATH '$[1]', c double PATH '$[2]', " +
                        "numItems integer PATH '$[3]')) hist  where column_name = ?;");
                ps.setString(1, attribute);
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    Bucket bucket = new Bucket();
                    bucket.setAttribute(attribute);
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
        } else if(attribute_type.equalsIgnoreCase("Date") && histogram_type.equalsIgnoreCase("equiheight")){
            try {
                ps = conn.prepareStatement("SELECT lvalue, uvalue, concat(round(c*100,1),'%') cumulfreq, " +
                        "CONCAT(round((c - LAG(c, 1, 0) over()) * 100,1), '%') freq, numItems " +
                        "FROM information_schema.column_statistics, JSON_TABLE(histogram->'$.buckets',       " +
                        "'$[*]' COLUMNS(lvalue date PATH '$[0]', uvalue date PATH '$[1]', c double PATH '$[2]', " +
                        "numItems integer PATH '$[3]')) hist  where column_name = ?;");
                ps.setString(1, attribute);
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    Bucket bucket = new Bucket();
                    bucket.setAttribute(attribute);
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
        } else if (attribute_type.equalsIgnoreCase("Double") && histogram_type.equalsIgnoreCase("equiheight")) {
            try {
                ps = conn.prepareStatement("SELECT lvalue, uvalue, concat(round(c*100,1),'%') cumulfreq, " +
                        "CONCAT(round((c - LAG(c, 1, 0) over()) * 100,1), '%') freq, numItems " +
                        "FROM information_schema.column_statistics, JSON_TABLE(histogram->'$.buckets',       " +
                        "'$[*]' COLUMNS(lvalue double PATH '$[0]', uvalue double PATH '$[1]', c double PATH '$[2]', " +
                        "numItems integer PATH '$[3]')) hist  where column_name = ? and lvalue is NOT NULL;");
                ps.setString(1, attribute);
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    Bucket bucket = new Bucket();
                    bucket.setAttribute(attribute);
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
        }
        else {
            throw new PolicyEngineException("Unknown Histogram type");
        }
        return hBuckets;
    }

    //TODO: change it from a static method to run automatically if the json files are not present
    public static void writeBuckets() {
        //For WiFiDataSet
//        writer.writeJSONToFile(getHistogram(PolicyConstants.START_DATE, "Date", "singleton"),
//                PolicyConstants.HISTOGRAM_DIR, PolicyConstants.START_DATE);
//        writer.writeJSONToFile(getHistogram(PolicyConstants.START_TIME, "Time", "equiheight"),
//                PolicyConstants.HISTOGRAM_DIR, PolicyConstants.START_TIME);
//        writer.writeJSONToFile(getHistogram(PolicyConstants.USERID_ATTR, "Integer", "equiheight"),
//                PolicyConstants.HISTOGRAM_DIR, PolicyConstants.USERID_ATTR);
//        writer.writeJSONToFile(getHistogram(PolicyConstants.LOCATIONID_ATTR, "String", "singleton"),
//                PolicyConstants.HISTOGRAM_DIR, PolicyConstants.LOCATIONID_ATTR);
//        writer.writeJSONToFile(getHistogram(PolicyConstants.GROUP_ATTR, "String", "singleton"),
//                PolicyConstants.HISTOGRAM_DIR, PolicyConstants.GROUP_ATTR);
//        writer.writeJSONToFile(getHistogram(PolicyConstants.PROFILE_ATTR, "String", "singleton"),
//                PolicyConstants.HISTOGRAM_DIR, PolicyConstants.PROFILE_ATTR);
        //For Orders table from TPCH
        writer.writeJSONToFile(getHistogram(PolicyConstants.ORDER_CUSTOMER_KEY, "Integer", "equiheight"),
                PolicyConstants.HISTOGRAM_DIR, PolicyConstants.ORDER_CUSTOMER_KEY);
        writer.writeJSONToFile(getHistogram(PolicyConstants.ORDER_PRIORITY, "String", "singleton"),
                PolicyConstants.HISTOGRAM_DIR, PolicyConstants.ORDER_PRIORITY);
        writer.writeJSONToFile(getHistogram(PolicyConstants.ORDER_CLERK, "String", "singleton"),
                PolicyConstants.HISTOGRAM_DIR, PolicyConstants.ORDER_CLERK);
        writer.writeJSONToFile(getHistogram(PolicyConstants.ORDER_PROFILE, "String", "singleton"),
                PolicyConstants.HISTOGRAM_DIR, PolicyConstants.ORDER_PROFILE);
        writer.writeJSONToFile(getHistogram(PolicyConstants.ORDER_DATE, "Date", "equiheight"),
                PolicyConstants.HISTOGRAM_DIR, PolicyConstants.ORDER_DATE);
        writer.writeJSONToFile(getHistogram(PolicyConstants.ORDER_TOTAL_PRICE, "Double", "equiheight"),
                PolicyConstants.HISTOGRAM_DIR, PolicyConstants.ORDER_TOTAL_PRICE);
    }

    public Map<String, List<Bucket>> getBucketMap() {
        return bucketMap;
    }

    private void retrieveBuckets(List<String> attribute_names) {
        bucketMap = new HashMap<>();
        for (String attribute : attribute_names) {
            bucketMap.put(attribute, sortBuckets(parseJSONList
                    (Reader.readTxt(PolicyConstants.HISTOGRAM_DIR + attribute + ".json"))));
        }
    }

    public List<Bucket> sortBuckets(List<Bucket> buckets) {
        Collections.sort(buckets);
        return buckets;
    }


    public List<Bucket> parseJSONList(String jsonData) {
        ObjectMapper objectMapper = new ObjectMapper();
        List<Bucket> buckets = null;
        try {
            buckets = objectMapper.readValue(jsonData, new TypeReference<List<Bucket>>() {
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        return buckets;
    }
}
