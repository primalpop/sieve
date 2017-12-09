package edu.uci.ics.tippers.data;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibatis.common.jdbc.ScriptRunner;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.model.tippers.Infrastructure;
import edu.uci.ics.tippers.model.tippers.User;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * Created by cygnus on 12/7/17.
 */
public class DataGeneration {


    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private Connection connection;

    private JSONParser parser;

    private String dataDir;

    public DataGeneration(String dataDir){
        this.dataDir = dataDir;
        this.connection = MySQLConnectionManager.getInstance().getConnection();
        this.parser = new JSONParser();
    }

    private void runScript(String fileName) throws PolicyEngineException {
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

    public void generateInfrastructure(){

        PreparedStatement stmt;
        String insert;

        String jsonData = edu.uci.ics.tippers.fileop.Reader.readFile(dataDir + DataFiles.INFRA.getPath());
        jsonData = jsonData.replace("\uFEFF", "");

        try{
            insert = "INSERT INTO INFRASTRUCTURE " +
                    "(ID, INFRASTRUCTURE_TYPE, NAME, FLOOR, REGION_NAME) VALUES (?, ?, ?, ?, ?)";


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
                stmt.setString(1, String.valueOf(infrastructures.get(i).getLocation_id()));
                stmt.setString(2, infrastructures.get(i).getType());
                stmt.setString(3, infrastructures.get(i).getName());
                stmt.setInt(4, Integer.parseInt(infrastructures.get(i).getName().substring(0,1)));
                stmt.setString(5, (String) infrastructures.get(i).getRegion_name());
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

            for (User user: users) {
                userStmt.setString(2, user.getEmail());
                userStmt.setString(3, user.getName());
                userStmt.setInt(1, user.getUser_id());
                userStmt.setString(4, user.getOffice());
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

        String jsonData = edu.uci.ics.tippers.fileop.Reader.readFile(dataDir + DataFiles.SO.getPath());
        jsonData = jsonData.replace("\uFEFF", "");

        String soInsert = "INSERT INTO SEMANTIC_OBSERVATION " +
                "(ID, USER_ID, LOCATION_ID, TEMPERATURE, ENERGY, ACTIVITY, TIMESTAMP) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";

        try {

            PreparedStatement userStmt = connection.prepareStatement(soInsert);
            int userCount = 0;

            ObjectMapper objectMapper = new ObjectMapper();
            List<User> users = null;
            try {
                users = objectMapper.readValue(jsonData,
                        new TypeReference<List<User>>(){});
            } catch (IOException e) {
                e.printStackTrace();
            }

            for (User user: users) {
                userStmt.setString(2, user.getEmail());
                userStmt.setString(3, user.getName());
                userStmt.setInt(1, user.getUser_id());
                userStmt.setString(4, user.getOffice());
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




    }

    public static void main (String [] args){
        DataGeneration dataGeneration = new DataGeneration("/data/");
        dataGeneration.runScript("mysql/schema.sql");
        dataGeneration.generateAll();
//        dataGeneration.runScript("mysql/drop.sql");
    }

}
