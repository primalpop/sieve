package edu.uci.ics.tippers.data;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.tippers.model.query.BasicQuery;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Author primpap
 */
public class PolicyGeneration {

    /**
     * Select all users
     * Select all locations
     * Select lowest and highest timestamp and generate a random timestamp between them
     * http://wcstitbits.blogspot.com/2012/05/generating-random-datetime-between-two.html
     * Select random wemo
     * Select random temperature
     * Append them together as json
     */


    public void writeToFile(String fileName, List<BasicQuery> basicQueries){

        ObjectMapper mapper = new ObjectMapper();

        for (BasicQuery q: basicQueries) {

            try {
                //Convert object to JSON string and save into file directly
                mapper.writeValue(new File(""), q);

                //Convert object to JSON string
                String jsonInString = mapper.writeValueAsString(q);
                System.out.println(jsonInString);

                //Convert object to JSON string and pretty print
                jsonInString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(q);
                System.out.println(jsonInString);


            } catch (JsonGenerationException e) {
                e.printStackTrace();
            } catch (JsonMappingException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
    
    public void generatePolicy(){

    }
}
