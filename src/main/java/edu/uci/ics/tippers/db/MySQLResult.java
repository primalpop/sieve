package edu.uci.ics.tippers.db;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.model.data.Semantic_Observation;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class MySQLResult {

    ResultSet resultSet;

    Duration duration;

    public MySQLResult(Duration duration) {
        this.duration = duration;
    }

    public MySQLResult(ResultSet resultSet, Duration duration) {
        this.resultSet = resultSet;
        this.duration = duration;
    }

    public ResultSet getResultSet() {
        return resultSet;
    }

    public void setResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    public Duration getDuration() {
        return duration;
    }

    public void setDuration(Duration duration) {
        this.duration = duration;
    }

    public void writeResultsToFile(String fileName){
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
        if(fileName!= null)
            writer.writeJSONToFile(query_results, PolicyConstants.QUERY_RESULTS_DIR, fileName);
    }
}
