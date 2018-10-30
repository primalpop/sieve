package edu.uci.ics.tippers.fileop;

import au.com.bytecode.opencsv.CSVWriter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import edu.uci.ics.tippers.common.PolicyConstants;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by cygnus on 10/29/17.
 */
public class Writer {

    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void writeResultsToCSV(ResultSet rs, String fileName ){
        Boolean includeHeaders = false;
        CSVWriter writer = null;
        try {
            writer = new CSVWriter(new FileWriter(fileName + ".csv"), '\t');
            writer.writeAll(rs, includeHeaders);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public void writeJSONToFile(List<?> items, String dir, String filename){
        ObjectMapper mapper = new ObjectMapper();
        mapper.setDateFormat(formatter);
        ObjectWriter writer = mapper.writer(new DefaultPrettyPrinter());
        File f = null;
        try {
            if(filename == null) {
                f = new File(dir + "policy" + items.size() + ".json");
                if (f.exists()) f = new File(dir + "policy" + items.size() + "-af.json");
            }
            else {
                f = new File(dir + "policy" + items.size() + filename +".json");
            }
            writer.writeValue(f, items);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createTextReport(TreeMap<String, Duration> runTimes, String fileDir) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter( fileDir + "results.csv"));

            String line = "";
            for (String policy: runTimes.keySet()) {
                String policyNumber = policy.replaceAll("\\D+","");
                if (runTimes.get(policy).compareTo(PolicyConstants.MAX_DURATION) < 0 ){
                    line +=  String.format("%s, %s", policyNumber, runTimes.get(policy).toMillis());
                }
                else{
                    line +=  String.format("%s,  %s", policyNumber, "Timed out" );
                }
                line += "\n";
            }
            writer.write(line);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createCSVReport(TreeMap<String, Duration> runTimes, String fileDir, String fileName) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter( fileDir + fileName));

            String line = "";
            for (String epochNumber: runTimes.keySet()) {
                if (runTimes.get(epochNumber).compareTo(PolicyConstants.MAX_DURATION) < 0 ){
                    line +=  String.format("%s, %s", epochNumber, runTimes.get(epochNumber).toMillis());
                }
                else{
                    line +=  String.format("%s,  %s", epochNumber, "Timed out" );
                }
                line += "\n";
            }
            writer.write(line);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
