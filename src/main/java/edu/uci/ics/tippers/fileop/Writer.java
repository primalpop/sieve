package edu.uci.ics.tippers.fileop;

import au.com.bytecode.opencsv.CSVWriter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import edu.uci.ics.tippers.common.PolicyConstants;

import java.io.*;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.List;
import java.util.TreeMap;


public class Writer {

    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void writeResultsToCSV(ResultSet rs, String fileName) {
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


    public void writeJSONToFile(List<?> items, String dir, String filename) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setDateFormat(formatter);
        ObjectWriter writer = mapper.writer(new DefaultPrettyPrinter());
        File f = null;
        try {
            if (filename == null) {
                f = new File(String.valueOf(Paths.get(dir, "policy" + items.size() + ".json")));
                if (f.exists()) f = new File(dir + "policy" + items.size() + "-af.json");
            } else {
                f = new File(String.valueOf(Paths.get(dir, filename + ".json")));
            }
            writer.writeValue(f, items);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createTextReport(TreeMap<String, Duration> runTimes, String fileDir) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(fileDir + "results.csv"));

            String line = "";
            for (String policy : runTimes.keySet()) {
//                String policyNumber = policy.replaceAll("\\D+","");
                if (runTimes.get(policy).compareTo(PolicyConstants.MAX_DURATION) < 0) {
                    line += String.format("%s, %s", policy, runTimes.get(policy).toMillis());
                } else {
                    line += String.format("%s,  %s", policy, "Timed out");
                }
                line += "\n";
            }
            writer.write(line);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void appendToCSVReport(TreeMap<String, String> runTimes, String fileDir, String fileName) {
        try {

            BufferedWriter writer = new BufferedWriter(new FileWriter(fileDir + fileName, true));
            String line = "";
            for (String number : runTimes.keySet()) {
                if (Long.parseLong(runTimes.get(number)) != PolicyConstants.MAX_DURATION.toMillis()) {
                    line += String.format("%s, %s", number, runTimes.get(number));
                } else {
                    line += String.format("%s,  %s", number, "Timed out");
                }
                line += "\n";
            }
            writer.write(line);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void addGuardReport(List<String> guardStrings, String fileDir, String fileName) {
        String FILE_HEADER =
                "Number of policies in the partition, " +
                        "Number of predicates in policies, " +
                        "Benefit, " +
                        "Utility, " +
                        "Coverage_of_guard," +
                        "Coverage_of_guard_and_partition," +
                        "Ratio_of_coverage, " +
                        "Total_time," +
                        "Guard_and_Partition";
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(fileDir + fileName, true));
            writer.write(FILE_HEADER);
            writer.write("\n");
            for (String g : guardStrings) {
                writer.write(g);
                writer.write("\n");
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeString(String results, String fileDir, String fileName) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(fileDir + fileName, true));
            writer.write(results);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeToFile(List<String> entries, String fileName, String fileDir){
        FileWriter writer = null;
        try {
            File file = new File(fileDir + fileName);
            writer = new FileWriter(file);
            for(String entry: entries) {
                writer.write(entry);
                writer.write(",");
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
