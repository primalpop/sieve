package manager;

import model.policy.BEPolicy;
import model.policy.ObjectCondition;
import model.policy.QuerierCondition;
import model.policy.RelOperator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by cygnus on 9/25/17.
 */
public class RunMe {

    public static void main(String args[]){

        BEPolicy policy = BEPolicy.parseJSONObject(readFile("/policy0.json"));

        policy.printPolicy();

        List<BEPolicy> policies = BEPolicy.parseJSONList(readFile("/policy1.json"));
    }

    public static String readFile(String filename) {
        String result = "";
        try {
            InputStream is = RunMe.class.getResourceAsStream(filename);
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();
            while (line != null) {
                sb.append(line);
                line = br.readLine();
            }
            result = sb.toString();
        } catch(Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}
