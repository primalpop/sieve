package edu.uci.ics.tippers.data;

import com.opencsv.CSVReader;
import edu.uci.ics.tippers.model.data.UserGroup;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class GroupGeneration {


    public void readCSVFile(String csvFile){
        String line = "";
        String csvSplitBy = ",";

        CSVReader csvReader = new CSVReader(new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(csvFile))));

        List<UserGroup> userGroups = new ArrayList<UserGroup>();

        String record = null;

       

    }

}
