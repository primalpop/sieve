package edu.uci.ics.tippers.generation.data.WiFi;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibatis.common.jdbc.ScriptRunner;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import edu.uci.ics.tippers.dbms.mysql.MySQLConnectionManager;
import edu.uci.ics.tippers.fileop.BigJsonReader;
import edu.uci.ics.tippers.model.tippers.dontuse.Infrastructure;
import edu.uci.ics.tippers.model.tippers.dontuse.SemanticObservation;
import edu.uci.ics.tippers.model.tippers.dontuse.User;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;


public class DataGeneration {
    
    private Connection connection;

    private JSONParser parser;

    private static String dataDir = "/metadata/";

    public DataGeneration(){
        this.connection = MySQLConnectionManager.getInstance().getConnection();
        try {
            this.connection.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        this.parser = new JSONParser();
    }

    public void runScript(String fileName) throws PolicyEngineException {
        ScriptRunner sr = new ScriptRunner(connection, false, true);
        sr.setLogWriter(null);
        Reader reader;
        try {
            InputStream inputStream = DataGeneration.class.getClassLoader().getResourceAsStream(fileName);
            reader = new BufferedReader(new InputStreamReader(inputStream));
            sr.runScript(reader);
        } catch (SQLException | IOException e) {
            e.printStackTrace();
            throw new PolicyEngineException("Error Running SQL script");
        }
    }


    public static List<Infrastructure> getAllInfra(){
        String jsonData = edu.uci.ics.tippers.fileop.Reader.readFile(dataDir + DataFiles.INFRA.getPath());
        jsonData = jsonData.replace("\uFEFF", "");

        ObjectMapper objectMapper = new ObjectMapper();
        List<Infrastructure> infrastructures = null;
        try {
            infrastructures = objectMapper.readValue(jsonData,
                    new TypeReference<List<Infrastructure>>() {
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
        return infrastructures;
    }

    public static List<User> getAllUser() {
        String jsonData = edu.uci.ics.tippers.fileop.Reader.readFile(dataDir + DataFiles.USER.getPath());
        jsonData = jsonData.replace("\uFEFF", "");


        ObjectMapper objectMapper = new ObjectMapper();
        List<User> users = null;
        try {
            users = objectMapper.readValue(jsonData,
                    new TypeReference<List<User>>() {
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
        return users;
    }


    public static HashMap<String, String> createActivity(List<Infrastructure> infrastructures){

        HashMap activityMap = new HashMap<String, String>();

        String activity = null;
        for (Infrastructure infra: infrastructures) {
            if (Stream.of("classroom", "class_room").anyMatch(infra.getType()::equalsIgnoreCase)){
                activity = "class";
            }
            else if (Stream.of("lounge", "meeting_room", "conference_room").anyMatch(infra.getType()::equalsIgnoreCase)) {
                activity = "meeting";
            }
            else if (Stream.of("credentials/lab", "faculty_office", "office").anyMatch(infra.getType()::equalsIgnoreCase)){
                activity = "work";
            }
            else if (Stream.of("male_restroom", "female_restroom", "kitchen").anyMatch(infra.getType()::equalsIgnoreCase)){
                activity = "private";
            }
            else if ((Stream.of("corridor", "utilities", "mail_room", "reception").anyMatch(infra.getType()::equalsIgnoreCase))) {
                activity = "walking";
            }
            else if ((Stream.of("seminar_room").anyMatch(infra.getType()::equalsIgnoreCase))) {
                activity = "seminar";
            }
            else{
                activity = "unknown";
            }
            activityMap.put(infra.getName(), activity);
        }
        return activityMap;
    }

    public void generateInfrastructure(){

        PreparedStatement stmt;
        String insert;

        String jsonData = edu.uci.ics.tippers.fileop.Reader.readFile(dataDir + DataFiles.INFRA.getPath());
        jsonData = jsonData.replace("\uFEFF", "");

        try{
            insert = "INSERT INTO INFRASTRUCTURE " +
                    "(INFRASTRUCTURE_TYPE, NAME, FLOOR, REGION_NAME) VALUES (?, ?, ?, ?)";


            ObjectMapper objectMapper = new ObjectMapper();
            List<Infrastructure> infrastructures = null;
            try {
                infrastructures = objectMapper.readValue(jsonData,
                        new TypeReference<List<Infrastructure>>(){});
            } catch (IOException e) {
                e.printStackTrace();
            }

            stmt = connection.prepareStatement(insert);
            for(int i =0;i<infrastructures.size();i++){
                stmt.setString(1, infrastructures.get(i).getType());
                stmt.setString(2, infrastructures.get(i).getName());
                stmt.setInt(3, Integer.parseInt(infrastructures.get(i).getName().substring(0,1)));
                stmt.setString(4, (String) infrastructures.get(i).getRegion_name());
                stmt.executeUpdate();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void generateUserGroups(){

        PreparedStatement stmt;
        String insert;

        int number_of_groups = 20;

        try{
            insert = "INSERT INTO USER_GROUP " +
                    "(ID, DESCRIPTION, NAME, OWNER) VALUES (?, ?, ?, ?)";


            stmt = connection.prepareStatement(insert);
            for(int i =0;i<number_of_groups;i++){
                stmt.setInt(1, i);
                stmt.setString(2, "Simulated group " + i);
                stmt.setString(3, "Sim"+ i);
                stmt.setString(4, null);
                stmt.executeUpdate();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Update users to add dummy names, emails and random offices from infrastructure if it doesn't exist in the JSON object
     */
    public void generateUsers() {

        String jsonData = edu.uci.ics.tippers.fileop.Reader.readFile(dataDir + DataFiles.USER.getPath());
        jsonData = jsonData.replace("\uFEFF", "");


        String userInsert = "INSERT INTO USER " +
                "(ID,EMAIL, NAME, OFFICE) VALUES (?, ?, ?, ?)";

        try {

            PreparedStatement userStmt = connection.prepareStatement(userInsert);
            int userCount = 0;

            ObjectMapper objectMapper = new ObjectMapper();
            List<User> users = null;
            try {
                users = objectMapper.readValue(jsonData,
                        new TypeReference<List<User>>(){});
            } catch (IOException e) {
                e.printStackTrace();
            }

            List<Infrastructure> infras = getAllInfra();

            for (User user: users) {
                userStmt.setString(2, user.getEmail() == null || user.getEmail().isEmpty() ? "virt"+userCount+"@tippers.com": user.getEmail());
                userStmt.setString(3, user.getName() == null || user.getName().isEmpty() ? "virt"+userCount: user.getName());
                userStmt.setInt(1, user.getUser_id());
                Random rand = new Random();
                Infrastructure randomOffice = infras.get(rand.nextInt(infras.size()));
                String office = null;
                if(user.getOffice() != null) {
                    office = infras.stream()
                            .filter(x -> user.getOffice().equals(x.getLocation_id()))
                            .map(Infrastructure::getName)
                            .findAny().toString();
                }
                else{
                    office = randomOffice.getName();
                }
                userStmt.setString(4, office);
                userStmt.addBatch();
                userCount++;
                if (userCount % PolicyConstants.BATCH_SIZE_INSERTION == 0) {
                    userStmt.executeBatch();
                    System.out.println("Inserted " + userCount + " records");
                }
            }
            userStmt.executeBatch();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        //Adding user group memberships
    }
    

    public void generateSemanticObservations(){


        Random r = new Random();
        int lowTemp = 55;
        int highTemp = 75;
        int lowWemo = 0;
        int highWemo = 100;

        HashMap<String, String> activityMap = createActivity(getAllInfra());

        String soInsert = "INSERT INTO SEMANTIC_OBSERVATION " +
                "(ID, USER_ID, LOCATION_ID, TEMPERATURE, ENERGY, ACTIVITY, TIMESTAMP) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";

        try {

            PreparedStatement presenceStmt = connection.prepareStatement(soInsert);
            int presenceCount = 0;

            BigJsonReader<SemanticObservation> reader = new BigJsonReader<>(dataDir + DataFiles.SO_FULL.getPath(),
                    SemanticObservation.class);
            SemanticObservation sobs = null;


            while ((sobs = reader.readNext()) != null) {
                presenceStmt.setString(1, String.valueOf(sobs.getId()));
                presenceStmt.setString(2, String.valueOf(sobs.getSemantic_entity_id()));
                presenceStmt.setString(3, sobs.getPayload());
                presenceStmt.setString(4, String.valueOf(r.nextInt(highTemp - lowTemp) + lowTemp));
                presenceStmt.setString(5, String.valueOf(r.nextInt(highWemo - lowWemo) + lowWemo));
                presenceStmt.setString(6, activityMap.get(sobs.getPayload()));
                presenceStmt.setTimestamp(7, sobs.getTimeStamp());
                presenceStmt.addBatch();
                presenceCount ++;

                if (presenceCount % PolicyConstants.BATCH_SIZE_INSERTION == 0) {
                    presenceStmt.executeBatch();
                    System.out.println("# " + presenceCount + " inserted");
//                    presenceStmt.close(); // needed for postgres
                }
            }

            presenceStmt.executeBatch();
            presenceStmt.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void generateAll() throws PolicyEngineException{
        try {
            connection.setAutoCommit(false);
//            generateInfrastructure();
//            generateUserGroups();
//            generateUsers();
            generateSemanticObservations();
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main (String [] args){
        DataGeneration dataGeneration = new DataGeneration();
        dataGeneration.runScript("realtest/wifi_schema.sql");
//        dataGeneration.generateAll();
//        dataGeneration.runScript("mysql/drop.sql");
    }

}
