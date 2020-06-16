package edu.uci.ics.tippers.dbms;

import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.model.data.LongPresence;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.stream.IntStream;

public class QueryResult {

    String pathName;
    String fileName;
    Duration timeTaken;
    int resultCount;
    List<LongPresence> queryResult;
    Boolean resultsCheck;

    public QueryResult() {
        this.timeTaken = Duration.ofMillis(0);
        this.resultCount = 0;
        this.resultsCheck = false;
    }

    public Boolean getResultsCheck() {
        return resultsCheck;
    }

    public void setResultsCheck(Boolean resultsCheck) {
        this.resultsCheck = resultsCheck;
    }


    public List<LongPresence> getQueryResult() {
        return queryResult;
    }

    public void setQueryResult(ResultSet resultSet) {
        this.queryResult = new ArrayList<>();
        Writer writer = new Writer();
        try{
            while(resultSet.next()){
                LongPresence so = new LongPresence();
                so.setId(resultSet.getString("id"));
//                so.setUser_id(resultSet.getString("user_id"));
//                so.setLocation(resultSet.getString("location_id"));
//                so.setTimeStamp(resultSet.getString("timeStamp"));
//                so.setTemperature(resultSet.getString("temperature"));
//                so.setEnergy(resultSet.getString("energy"));
//                so.setActivity(resultSet.getString("activity"));
                this.queryResult.add(so);
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        try {
            resultSet.beforeFirst();
        } catch (SQLException e) {
            e.printStackTrace();
        }
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

    public QueryResult(String pathName, String fileName) {
        this.pathName = pathName;
        this.fileName = fileName;
    }

    public void writeResultsToFile(ResultSet resultSet){

        List<LongPresence> query_results = new ArrayList<>();
        Writer writer = new Writer();
        try{
            while(resultSet.next()){
                LongPresence so = new LongPresence();
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

    public Boolean checkResults(QueryResult otherResult) {
        List<LongPresence> og = this.getQueryResult();
        List<LongPresence> tbc = otherResult.getQueryResult();
        if(og.size() != tbc.size()) {
            System.out.println("Not of same size: " + og.size() + " != " + tbc.size());
            System.out.println("Set size: " + new HashSet<>(og).size() + " " + new HashSet<>(tbc).size());
            return false;
        }
        Comparator<LongPresence> comp = Comparator.comparingInt(so -> Integer.parseInt(so.getId()));
        og.sort(comp);
        tbc.sort(comp);
        return IntStream.range(0, og.size())
                .allMatch(i -> comp.compare(og.get(i), tbc.get(i)) == 0);
    }
}
