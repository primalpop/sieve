package edu.uci.ics.tippers.db;

import com.fasterxml.jackson.core.type.TypeReference;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import edu.uci.ics.tippers.fileop.Reader;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.model.guard.Bucket;

import java.io.*;
import java.security.Policy;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;

public class Histogram {

    private static java.sql.Connection conn = MySQLConnectionManager.getInstance().getConnection();

    private static Writer writer = new Writer();

    private static Map<String, List<Bucket>> bucketMap;

    private static Histogram _instance;

    private Histogram(){
        retrieveBuckets();
    }

    public static Histogram getInstance(){
        if (_instance == null)
            _instance = new Histogram();
        return _instance;
    }

    public Map<String, List<Bucket>> getBucketMap(){
        return bucketMap;
    }

    private void retrieveBuckets(){
        bucketMap = new HashMap<>();
        bucketMap.put(PolicyConstants.USERID_ATTR, sortBuckets(parseJSONList(Reader.readTxt(PolicyConstants.HISTOGRAM_DIR + PolicyConstants.USERID_ATTR + ".json"))));
        bucketMap.put(PolicyConstants.TIMESTAMP_ATTR, sortBuckets(parseJSONList(Reader.readTxt(PolicyConstants.HISTOGRAM_DIR + PolicyConstants.TIMESTAMP_ATTR + ".json"))));
        bucketMap.put(PolicyConstants.LOCATIONID_ATTR, sortBuckets( parseJSONList(Reader.readTxt(PolicyConstants.HISTOGRAM_DIR + PolicyConstants.LOCATIONID_ATTR + ".json"))));
        bucketMap.put(PolicyConstants.ENERGY_ATTR, sortBuckets(parseJSONList(Reader.readTxt(PolicyConstants.HISTOGRAM_DIR + PolicyConstants.ENERGY_ATTR + ".json"))));
        bucketMap.put(PolicyConstants.TEMPERATURE_ATTR, sortBuckets( parseJSONList(Reader.readTxt(PolicyConstants.HISTOGRAM_DIR + PolicyConstants.TEMPERATURE_ATTR + ".json"))));
        bucketMap.put(PolicyConstants.ACTIVITY_ATTR, sortBuckets(parseJSONList(Reader.readTxt(PolicyConstants.HISTOGRAM_DIR + PolicyConstants.ACTIVITY_ATTR + ".json"))));
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
        } else {
            throw new PolicyEngineException("Unknown Histogram type");
        }
        return hBuckets;
    }

    //TODO: change it from a static method to run automatically if the json files are not present
    public static void writeBuckets(){
        writer.writeJSONToFile(getHistogram("user_id", "String", "equiheight"), PolicyConstants.HISTOGRAM_DIR, PolicyConstants.USERID_ATTR);
        writer.writeJSONToFile(getHistogram("timeStamp", "DateTime", "equiheight"), PolicyConstants.HISTOGRAM_DIR, PolicyConstants.TIMESTAMP_ATTR);
        writer.writeJSONToFile(getHistogram("location_id", "String", "singleton"), PolicyConstants.HISTOGRAM_DIR, PolicyConstants.LOCATIONID_ATTR);
        writer.writeJSONToFile(getHistogram("energy", "String", "singleton"), PolicyConstants.HISTOGRAM_DIR, PolicyConstants.ENERGY_ATTR);
        writer.writeJSONToFile(getHistogram("temperature", "String", "singleton"), PolicyConstants.HISTOGRAM_DIR, PolicyConstants.TEMPERATURE_ATTR);
        writer.writeJSONToFile(getHistogram("activity", "String", "singleton"), PolicyConstants.HISTOGRAM_DIR, PolicyConstants.ACTIVITY_ATTR);
    }

    public List<Bucket> sortBuckets(List<Bucket> buckets){
        Collections.sort(buckets);
        return buckets;
    }


    public List<Bucket> parseJSONList(String jsonData) {
        ObjectMapper objectMapper = new ObjectMapper();
        List<Bucket> buckets = null;
        try {
            buckets = objectMapper.readValue(jsonData, new TypeReference<List<Bucket>>(){});
        } catch (IOException e) {
            e.printStackTrace();
        }
        return buckets;
    }
}
