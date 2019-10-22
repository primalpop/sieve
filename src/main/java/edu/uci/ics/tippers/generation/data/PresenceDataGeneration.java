package edu.uci.ics.tippers.generation.data;

import com.opencsv.CSVReader;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.model.data.Presence;
import org.json.JSONObject;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class PresenceDataGeneration {

    private static final String dataDir = "/data/";
    private Connection connection;
    private HashMap<String, Integer> users;

    public PresenceDataGeneration(){
        this.connection = MySQLConnectionManager.getInstance().getConnection();
        try {
            this.connection.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        this.users = new HashMap<>();
    }

    private void getAllUsers() {
        PreparedStatement queryStm = null;
        try {
            queryStm = connection.prepareStatement("SELECT ID, USER_ID as id, user_id " +
                    "FROM USER");
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) users.put(rs.getString("user_id"), rs.getInt("id"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void readCoverage(){
        try{
            BufferedReader br = new BufferedReader (new InputStreamReader(
                    this.getClass().getResourceAsStream( dataDir + DataFiles.COVERAGE.getPath())));
            String st;
            while ((st = br.readLine()) != null) {
                if(st.startsWith("3")){
                    String[] arr = st.split("\\|");
                    String[] roomsWithSpace = arr[1].split(",");
                    List<String> rooms = new ArrayList<String>();
                    for (String r: roomsWithSpace){
                        if (r.startsWith(" "))
                            rooms.add(r.substring(1));
                        rooms.add(r);
                    }
                    String[] rs = new String[rooms.size()];
                    rs = rooms.toArray(rs);
                }
            }
        }
        catch (IOException io) {
            io.printStackTrace();
        }
    }

    private Timestamp parseTimeStamp(String ts){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date parsedDate = null;
        try {
            parsedDate = dateFormat.parse(ts);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return new Timestamp(parsedDate.getTime());
    }


    private void parseAndWrite(String fileName) {

        String pInsert = "INSERT INTO PRESENCE " +
                "(USER_ID, LOCATION_ID, START, FINISH) " +
                "VALUES (?, ?, ?, ?)";
        int presenceCount = 0;
        try {
            InputStream in = getClass().getResourceAsStream(fileName);
            InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8);
            CSVReader csvReader = new CSVReader(reader, ',', '"', 1);
            String[] nextRecord;
            PreparedStatement presenceStmt = connection.prepareStatement(pInsert);
            while ((nextRecord = csvReader.readNext()) != null) {
                Random rand = new Random();
                Presence presence = new Presence();
                if(nextRecord.length == 6){
                    int user_id = users.get(new JSONObject(nextRecord[2]).getString("client_id"));
                    presence.setUser_id(user_id);
                    presence.setStart(parseTimeStamp(nextRecord[3]));
                    presence.setFinish(parseTimeStamp(nextRecord[4]));
                    presence.setLocation_id(nextRecord[5]);
                }
                else{
                    int user_id = users.get(new JSONObject(nextRecord[0]).getString("client_id"));
                    presence.setUser_id(user_id);
                    presence.setStart(parseTimeStamp(nextRecord[1]));
                    presence.setFinish(parseTimeStamp(nextRecord[2]));
                    presence.setLocation_id(nextRecord[3]);
                }
                presenceStmt.setInt(1, presence.getUser_id());
                presenceStmt.setString(2, presence.getLocation_id());
                presenceStmt.setTimestamp(3, presence.getStart());
                presenceStmt.setTimestamp(4, presence.getFinish());
                presenceStmt.addBatch();
                presenceCount++;
                if (presenceCount % PolicyConstants.BATCH_SIZE_INSERTION == 0) {
                    presenceStmt.executeBatch();
                    System.out.println("# " + presenceCount + " inserted");
//                    presenceStmt.close(); // needed for postgres
                }
            }
            presenceStmt.executeBatch();
            presenceStmt.close();
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        PresenceDataGeneration pdg = new PresenceDataGeneration();
        pdg.getAllUsers();
        pdg.parseAndWrite(dataDir + DataFiles.PRESENCE_REAL.getPath());
    }
}
