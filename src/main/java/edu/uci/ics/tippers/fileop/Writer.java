package edu.uci.ics.tippers.fileop;

import au.com.bytecode.opencsv.CSVWriter;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by cygnus on 10/29/17.
 */
public class Writer {

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


}
