package fileop;

import au.com.bytecode.opencsv.CSVWriter;
import execution.RunMe;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by cygnus on 10/29/17.
 */
public class Writer {

    private void writeResultsToCSV(ResultSet rs, int queryIndex){
        Boolean includeHeaders = false;
        CSVWriter writer = null;
        try {
            writer = new CSVWriter(new FileWriter("query" + queryIndex + ".csv"), '\t');
            writer.writeAll(rs, includeHeaders);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
