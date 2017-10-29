package model.query;

import au.com.bytecode.opencsv.CSVWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;

/**
 * Created by cygnus on 7/7/17.
 *
 * Includes Q1 to Q5 from experiments
 */
public class BasicQuery {

    public static final String [] queryList = {
            "SELECT count(*) FROM SEMANTIC_OBSERVATION where ",
            "Select from SEMANTIC_OBSERVATION where user_id = 2564 and ",
            "Select * from SEMANTIC_OBSERVATION where location = 2028",
            "Select user from SEMANTIC_OBSERVATION where timeStamp between " +
                    "\"0003-07-17 02:13:00\" and \"0003-08-17 18:06:00\" and ",
            "Select so.user_id from SEMANTIC_OBSERVATION as so, " +
                    "INFRASTRUCTURE as i, USER as u where so.user_id = u.user_id and " +
                    "i.name = so.location and u.group_name = \"ISG\" and i.type = \"lab\" and "
    };


}
