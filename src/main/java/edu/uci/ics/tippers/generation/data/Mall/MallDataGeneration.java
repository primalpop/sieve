package edu.uci.ics.tippers.generation.data.Mall;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.fileop.Reader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class MallDataGeneration {

    Map<Integer, List<MallShop>> wifiToShop;
    static Random r;

    static int MULTIPLIER = 10; //For number of customers
    static int MAX_DEVICE_ID = 268;
    static Map<Integer, String> userToInterest;
    static final List<String> interest = Arrays.asList("clothes", "shoes", "cosmetics", "arcade", "movies", "restaurant");

    public MallDataGeneration(){
        wifiToShop = new HashMap<>();
        r = new Random();
        userToInterest = new HashMap<>();
    }


    public List<MallObservation> read(Path path) {
        List<MallObservation> list = new ArrayList<>();
        try {
            InputStream is = Reader.class.getResourceAsStream(path.toString());
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(br);
            for (CSVRecord record : records) {
                String obs_no = record.get("observation_number");
                int ap = Integer.parseInt(record.get("wifi_ap"));
                LocalDateTime ldt = LocalDateTime.parse(record.get("timestamp"), DateTimeFormatter.ofPattern(PolicyConstants.TIMESTAMP_FORMAT));
                LocalDate obs_date = ldt.toLocalDate();
                LocalTime obs_time = ldt.toLocalTime();
                int device = Integer.parseInt(record.get("device_id"));
                MallShop ms = wifiToShop.get(ap).get(r.nextInt(wifiToShop.get(ap).size()));
                String shop_name = ms.shop_name;
                if(!userToInterest.containsKey(device)) {
                    String user_interest = Math.random() > 0.7? interest.get(r.nextInt(interest.size())): null;
                    userToInterest.put(device, user_interest);
                }
                MallObservation mallObservation = new MallObservation(obs_no, shop_name, obs_date, obs_time, userToInterest.get(device), device);
                list.add(mallObservation);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    public void writeToDB(List<MallObservation> mallObservations) {
        Connection connection = PolicyConstants.getDBMSConnection();
        String moInsert = "INSERT INTO MALL_OBSERVATION " +
                "(id, shop_name, obs_date, obs_time, user_interest, device_id) " +
                "VALUES (?, ?, ?, ?, ?, ?)";

        int i = 0;
        try {
            PreparedStatement moStmt = connection.prepareStatement(moInsert);
            for (MallObservation mo: mallObservations) {
                moStmt.setString(1, mo.getObservation_number());
                moStmt.setString(2, mo.getShop_name());
                moStmt.setDate(3, Date.valueOf(mo.getObs_date()));
                moStmt.setTime(4, Time.valueOf(mo.getObs_time()));
                moStmt.setString(5, mo.getUser_interest());
                moStmt.setInt(6, mo.getDevice());
                moStmt.addBatch();
                i++;
                if (i % PolicyConstants.BATCH_SIZE_INSERTION == 0) {
                    moStmt.executeBatch();
                    System.out.println("# " + i + " inserted");
                }
            }
            moStmt.executeBatch();
            moStmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public List<MallShop> readCoverage(){
        Path coverageInput = Paths.get("/metadata/mall_coverage.csv");
        List<MallShop> mallShops = new ArrayList<>();
        try {
            InputStream is = Reader.class.getResourceAsStream(coverageInput.toString());
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().withIgnoreSurroundingSpaces().
                    withNullString("").parse(br);
            for (CSVRecord record : records) {
                String name = record.get("name");
                int id = Integer.parseInt(record.get("id"));
                String type = record.get("type");
                int capacity = Integer.parseInt(record.get("capacity"));
                int wifiAp1 =  record.get("Wifi-ap-1") != null? Integer.parseInt(record.get("Wifi-ap-1")): 0;
                int wifiAp2 = record.get("Wifi-ap-2") != null? Integer.parseInt(record.get("Wifi-ap-2")): 0;
                int wifiAp3 = record.get("Wifi-ap-3") != null? Integer.parseInt(record.get("Wifi-ap-3")): 0;
                int wifiAp4 = record.get("Wifi-ap-4") != null? Integer.parseInt(record.get("Wifi-ap-4")): 0;
                List<Integer> wifiaps = new ArrayList<>();
                if (wifiAp1 != 0) wifiaps.add(wifiAp1);
                if (wifiAp2 != 0) wifiaps.add(wifiAp2);
                if (wifiAp3 != 0) wifiaps.add(wifiAp3);
                if (wifiAp4 != 0) wifiaps.add(wifiAp4);
                MallShop mallShop = new MallShop(name, id, type, capacity, wifiaps);
                mallShops.add(mallShop);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return mallShops;
    }


    public static void main(final String[] args) {
        PolicyConstants.initialize();
        MallDataGeneration mdg = new MallDataGeneration();
        List<MallShop> mallShops = mdg.readCoverage();
        for (MallShop ms: mallShops) {
            for (int i = 0; i < ms.wifiaps.size(); i++) {
                int wifiap = ms.wifiaps.get(i);
                if (!mdg.wifiToShop.containsKey(wifiap)){
                    List<MallShop> wifiMallShops = new ArrayList<>();
                    wifiMallShops.add(ms);
                    mdg.wifiToShop.put(wifiap, wifiMallShops);
                }
                else mdg.wifiToShop.get(wifiap).add(ms);
            }
        }
        Path pathInput = Paths.get("/metadata/mallObservations.csv");
        List<MallObservation> mallObservations = mdg.read(pathInput);
        Comparator<MallObservation> dateComparator = new Comparator<MallObservation>() {
            @Override
            public int compare(MallObservation o1, MallObservation o2) {
                return o1.getObs_date().compareTo(o2.getObs_date());
            }
        };
        mallObservations.sort(dateComparator);
        int window_size = mallObservations.size()/ MULTIPLIER;
        for (int i = window_size; i < mallObservations.size(); i++) {
            int extDevId = mallObservations.get(i - window_size).getDevice() + MAX_DEVICE_ID + 1;
            mallObservations.get(i).setDevice(extDevId);
            if(!userToInterest.containsKey(extDevId)){
                String user_interest = Math.random() > 0.7? interest.get(r.nextInt(interest.size())): null;
                userToInterest.put(extDevId, user_interest);
            }
            mallObservations.get(i).setUser_interest(userToInterest.get(extDevId));
            mallObservations.get(i).setObs_date(mallObservations.get(i - window_size).getObs_date());
            mallObservations.get(i).setObs_time(mallObservations.get(i - window_size).getObs_time());
        }
        mdg.writeToDB(mallObservations);
    }

}
