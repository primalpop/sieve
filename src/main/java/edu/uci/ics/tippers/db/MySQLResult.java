package edu.uci.ics.tippers.db;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.model.data.Semantic_Observation;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MySQLResult {

    String pathName;
    String fileName;
    Duration timeTaken;
    int resultCount;

    public MySQLResult() {
        this.timeTaken = Duration.ofMillis(0);
        this.resultCount = 0;
    }

    public int getResultCount() {
        return resultCount;
    }

    public void setResultCount(int resultCount) {
        this.resultCount = resultCount;
    }

    public Duration getTimeTaken() {
        return timeTaken;
    }

    public void setTimeTaken(Duration timeTaken) {
        this.timeTaken = timeTaken;
    }

    public String getPathName() {
        return pathName;
    }

    public void setPathName(String pathName) {
        this.pathName = pathName;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public MySQLResult(String pathName, String fileName) {
        this.pathName = pathName;
        this.fileName = fileName;
    }

    public void writeResultsToFile(ResultSet resultSet){

        List<Semantic_Observation> query_results = new ArrayList<>();
        Writer writer = new Writer();
        try{
            while(resultSet.next()){
                Semantic_Observation so = new Semantic_Observation();
                so.setId(resultSet.getString("id"));
                so.setUser_id(resultSet.getString("user_id"));
                so.setLocation(resultSet.getString("location_id"));
                so.setTimeStamp(resultSet.getString("timeStamp"));
                so.setTemperature(resultSet.getString("temperature"));
                so.setEnergy(resultSet.getString("energy"));
                so.setActivity(resultSet.getString("activity"));
                query_results.add(so);
            }
            resultSet.close();
        }catch (SQLException e){
            e.printStackTrace();
        }
        if(pathName != null && fileName != null)
            writer.writeJSONToFile(query_results, pathName, fileName);
    }
}
