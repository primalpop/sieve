package edu.uci.ics.tippers.generation.data.Mall;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class MallDataGeneration {

    //TODO: Update Mall data generation code based on discussion yesterday and new schema
    public List<MallObservation> read(Path path) {
        List<MallObservation> list = new ArrayList<>();
        try {
            // Read CSV file. For each row, instantiate and collect `Observation`.
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
                // Instantiate `Person` object, and collect it.
                MallObservation mallObservation = new MallObservation(obs_no, ap, obs_date, obs_time, device);
                list.add(mallObservation);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }


    public void writeToDB(List<MallObservation> mallObservations) {

        Connection connection = MySQLConnectionManager.getInstance().getConnection();

        String moInsert = "INSERT INTO MALL_OBSERVATION " +
                "(id, wifi_ap, obs_date, obs_time, device_id) " +
                "VALUES (?, ?, ?, ?, ?)";

        int i = 0;
        try {
            PreparedStatement moStmt = connection.prepareStatement(moInsert);
            for (MallObservation mo: mallObservations) {
                moStmt.setString(1, mo.getObservation_number());
                moStmt.setInt(2, mo.getWifi_ap());
                moStmt.setDate(3, Date.valueOf(mo.getObs_date()));
                moStmt.setTime(4, Time.valueOf(mo.getObs_time()));
                moStmt.setInt(5, mo.getDevice());
                moStmt.addBatch();
                i++;
                if (i % 10 == 0) { //TODO: Update this to BATCH_SIZE
                    moStmt.executeBatch();
                    System.out.println("# " + i + " inserted");
                    break;
                }
            }
            moStmt.executeBatch();
            moStmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static void main(final String[] args) {
        PolicyConstants.initialize();
        MallDataGeneration mdg = new MallDataGeneration();
        Path pathInput = Paths.get("/data/mallObservations.csv");
        List<MallObservation> observations = mdg.read(pathInput);
        System.out.println(observations.size());
        mdg.writeToDB(observations);
    }

}
