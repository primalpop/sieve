package edu.uci.ics.tippers.fileop;

import edu.uci.ics.tippers.execution.RunMe;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * Created by cygnus on 11/1/17.
 */
public class Reader {

    public static String readFile(String filename) {
        String result = "";
        try {
            InputStream is = Reader.class.getResourceAsStream(filename);
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
