package edu.uci.ics.tippers.dbms.mysql;

import com.fasterxml.jackson.core.type.TypeReference;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import edu.uci.ics.tippers.fileop.Reader;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.model.guard.Bucket;

import java.io.*;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Generate and write histograms to JSON files
 * Histogram Generation specific to MySQL
 */
public class Histogram {


    private static Writer writer = new Writer();

    private static Map<String, List<Bucket>> bucketMap;

    private static Histogram _instance;

    private static File histDirectory;

    private Histogram() {
        histDirectory = new File(String.valueOf(Paths.get(PolicyConstants.HISTOGRAM_DIR.toLowerCase(),
                PolicyConstants.TABLE_NAME.toLowerCase())));
        if (histDirectory.isDirectory() && Objects.requireNonNull(histDirectory.list()).length == 0)
            writeBuckets(PolicyConstants.TABLE_NAME);
        else retrieveBuckets(PolicyConstants.ATTRIBUTES);
    }

    public static Histogram getInstance() {
        if (_instance == null)
            _instance = new Histogram();
        return _instance;
    }

    private static List<Bucket> getHistogram(String attribute, String attribute_type, String histogram_type) {
        if(PolicyConstants.DBMS_CHOICE.equalsIgnoreCase(PolicyConstants.PGSQL_DBMS)) {
            throw new PolicyEngineException("Histogram generation only supported on MySQL");
        }
        Connection conn = MySQLConnectionManager.getInstance().getConnection();
        List<Bucket> hBuckets = new ArrayList<>();
        PreparedStatement ps = null;
        if (attribute_type.equalsIgnoreCase("String") && histogram_type.equalsIgnoreCase("singleton")) {
            try {
                if(PolicyConstants.TABLE_NAME.equalsIgnoreCase(PolicyConstants.WIFI_TABLE)) {
                    ps = conn.prepareStatement(
                            "SELECT FROM_BASE64(SUBSTRING_INDEX(v, ':', -1)) value, concat(round(c*100,1),'%') cumulfreq, " +
                                    "CONCAT(round((c - LAG(c, 1, 0) over()) * 100,1), '%') freq " +
                                    "FROM information_schema.column_statistics, JSON_TABLE(histogram->'$.buckets', " +
                                    "'$[*]' COLUMNS(v VARCHAR(60) PATH '$[0]', c double PATH '$[1]')) hist  " +
                                    "where column_name = ?;");
                }
                else { //Remove FROM_BASE64 conversion
                    ps = conn.prepareStatement(
                            "SELECT v as value, concat(round(c*100,1),'%') cumulfreq, " +
                                    "CONCAT(round((c - LAG(c, 1, 0) over()) * 100,1), '%') freq " +
                                    "FROM information_schema.column_statistics, JSON_TABLE(histogram->'$.buckets', " +
                                    "'$[*]' COLUMNS(v VARCHAR(60) PATH '$[0]', c double PATH '$[1]')) hist  " +
                                    "where column_name = ?;");
                }

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

    public void writeBuckets(String table_name) {
        String filePath = new File(String.valueOf(Paths.get(PolicyConstants.HISTOGRAM_DIR.toLowerCase(), table_name.toLowerCase()))).getPath();
        if(table_name.equalsIgnoreCase(PolicyConstants.WIFI_TABLE)) {
            writer.writeJSONToFile(getHistogram(PolicyConstants.START_DATE, "Date", "singleton"),
                    filePath, PolicyConstants.START_DATE);
            writer.writeJSONToFile(getHistogram(PolicyConstants.START_TIME, "Time", "equiheight"),
                    filePath, PolicyConstants.START_TIME);
            writer.writeJSONToFile(getHistogram(PolicyConstants.USERID_ATTR, "Integer", "equiheight"),
                    filePath, PolicyConstants.USERID_ATTR);
            writer.writeJSONToFile(getHistogram(PolicyConstants.LOCATIONID_ATTR, "String", "singleton"),
                    filePath, PolicyConstants.LOCATIONID_ATTR);
            writer.writeJSONToFile(getHistogram(PolicyConstants.GROUP_ATTR, "String", "singleton"),
                    filePath, PolicyConstants.GROUP_ATTR);
            writer.writeJSONToFile(getHistogram(PolicyConstants.PROFILE_ATTR, "String", "singleton"),
                    filePath, PolicyConstants.PROFILE_ATTR);
        }
        else if(table_name.equalsIgnoreCase(PolicyConstants.ORDERS_TABLE)) {
            writer.writeJSONToFile(getHistogram(PolicyConstants.ORDER_CUSTOMER_KEY, "Integer", "equiheight"),
                    filePath, PolicyConstants.ORDER_CUSTOMER_KEY);
            writer.writeJSONToFile(getHistogram(PolicyConstants.ORDER_PRIORITY, "String", "singleton"),
                    filePath, PolicyConstants.ORDER_PRIORITY);
            writer.writeJSONToFile(getHistogram(PolicyConstants.ORDER_CLERK, "String", "singleton"),
                    filePath, PolicyConstants.ORDER_CLERK);
            writer.writeJSONToFile(getHistogram(PolicyConstants.ORDER_PROFILE, "String", "singleton"),
                    filePath, PolicyConstants.ORDER_PROFILE);
            writer.writeJSONToFile(getHistogram(PolicyConstants.ORDER_DATE, "Date", "equiheight"),
                    filePath, PolicyConstants.ORDER_DATE);
            writer.writeJSONToFile(getHistogram(PolicyConstants.ORDER_TOTAL_PRICE, "Double", "equiheight"),
                    filePath, PolicyConstants.ORDER_TOTAL_PRICE);
        }
        else if(table_name.equalsIgnoreCase(PolicyConstants.MALL_TABLE)){
            writer.writeJSONToFile(getHistogram(PolicyConstants.M_SHOP_NAME, "String", "singleton"),
                    filePath, PolicyConstants.M_SHOP_NAME);
            writer.writeJSONToFile(getHistogram(PolicyConstants.M_DATE, "Date", "singleton"),
                    filePath, PolicyConstants.M_DATE);
            writer.writeJSONToFile(getHistogram(PolicyConstants.M_TIME, "Time", "equiheight"),
                    filePath, PolicyConstants.M_TIME);
            writer.writeJSONToFile(getHistogram(PolicyConstants.M_INTEREST, "String", "singleton"),
                    filePath, PolicyConstants.M_INTEREST);
            writer.writeJSONToFile(getHistogram(PolicyConstants.M_DEVICE, "Integer", "equiheight"),
                    filePath, PolicyConstants.M_DEVICE);
        }
    }

    public Map<String, List<Bucket>> getBucketMap() {
        return bucketMap;
    }

    private void retrieveBuckets(List<String> attribute_names) {
        bucketMap = new HashMap<>();
        for (String attribute : attribute_names) {
            bucketMap.put(attribute, sortBuckets(parseJSONList
                    (Reader.readTxt(String.valueOf(Paths.get(histDirectory.getPath(), attribute + ".json"))))));
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
