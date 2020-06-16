package edu.uci.ics.tippers.fileop;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import edu.uci.ics.tippers.common.PolicyEngineException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;

public class BigJsonReader<T> {

    private Class<?> claaz;
    private JsonReader reader;
    private Gson gson;
    private static String datePattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static SimpleDateFormat sdf = new SimpleDateFormat(datePattern);

    public BigJsonReader(String filePath, Class claaz) {
        this.claaz = claaz;
        try {
            InputStream is = BigJsonReader.class.getResourceAsStream(filePath);
            reader = new JsonReader(new InputStreamReader(is, "UTF-8"));
            reader.setLenient(true);
            gson = new GsonBuilder()
                    .setDateFormat(datePattern)
                    .registerTypeAdapter(JSONObject.class, Converter.getJSONDeserializer())
                    .create();
            reader.beginArray();
        } catch (IOException e) {
            e.printStackTrace();
            throw new PolicyEngineException("Error Reading Big Json File");
        }
    }

    public T readNext() {
        try {
            if (reader.hasNext()) {
                return gson.fromJson(reader, claaz);
            } else {
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new PolicyEngineException("Error Reading Big Json File");
        }
    }

}
