import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Observation {
	private List<String> timestamps;

    public Observation() {
        this.timestamps = new ArrayList<String>();
    }

    public void parseData() {
        // parse json file
        JSONArray observations = new JSONArray();
        try {
            JSONParser parser = new JSONParser();
            observations = (JSONArray) parser.parse(new FileReader("../POST/observation.json"));
        } catch (FileNotFoundException ex) {
            System.err.println("File not found");
            // exit(1); Problem
        } catch (IOException ex) {
            System.err.println("Input or out error");
            // exit(1);
        } catch (ParseException ex) {
            System.err.println("Parse error");
            // exit(1);
        }
        
        // loop through the observations array and extract data
        for (Object obj : observations) {
            JSONObject observation = (JSONObject) obj;
            String timestamp = (String) observation.get("timestamp");
            if (!this.timestamps.contains(timestamp)) {
                this.timestamps.add(timestamp);
            }
        }
    }

    public List<String> getTimestamps() {
        return timestamps;
    }
}
