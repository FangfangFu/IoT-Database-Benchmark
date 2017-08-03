// Author: Fangfang Fu
// Date: 8/2/2017
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;

import java.util.ArrayList;
import java.util.List;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import com.google.gson.Gson;
import com.google.gson.stream.*;
import java.util.Random;
import java.sql.Timestamp;
import java.text.SimpleDateFormat; 

public class ScaleData {

	public static void main(String[] args) throws FileNotFoundException {
		// parse temperatureObs file
		TemperatureObs temperatureObs = new TemperatureObs();
		temperatureObs.parseData();
		List<String> sensorTypeIds = temperatureObs.getTypeIds();
		List<String> sensorIds = temperatureObs.getSensorIds();
        List<Integer> payloadLimits = temperatureObs.getPayloadLimits();
        List<String> timestamps = temperatureObs.getTimestamps();
        int recordDays = temperatureObs.getRecordDays();
        int obsSpeed = temperatureObs.getObsSpeed();
        
        int seed = 1; // set up a constant seed
        
        // temporal scaling -- where we extend the data from same sensors for a long time
		int extendDays = 1;
		String fileName1 = "SimulatedData/simulatedTempObs1.json";
		String startTime1 = timeAddDays(timestamps.get(0), recordDays); // start from new date
		simulateTempObsData(sensorIds, sensorTypeIds.get(0), payloadLimits, obsSpeed, startTime1, extendDays, seed, fileName1);
		
		// speed scaling -- where same devices generate data, but at a faster speed
		int accelation = 2;
		int scaleSpeed = obsSpeed * accelation;
		String fileName2 = "SimulatedData/simulatedTempObs2.json";
		String startTime2 = timestamps.get(0); // start from the original date
		simulateTempObsData(sensorIds, sensorTypeIds.get(0), payloadLimits, scaleSpeed, startTime2, extendDays, seed, fileName2);
		
		// device scaling -- when we scale number of devices
		int scaleNum = 2;
		String simulatedName = "simulatedEmeter"; 
		List<String> scaledSensorIds = scaleSensorIds(sensorIds, scaleNum, simulatedName);
		String fileName3 = "SimulatedData/simulatedTempObs3.json";
		String startTime3 = timestamps.get(0); // start from the original date
		simulateTempObsData(scaledSensorIds, sensorTypeIds.get(0), payloadLimits, obsSpeed, startTime3, extendDays, seed, fileName3);
		
		// createNewFileWithUpdatedContent(seed, payloadLimits, extendDays);
	}
	
//	public static void speedScale(List<String> sensorIds, String sensorType, List<Integer> payloadLimits, 
//			int acceration, int obsSpeed, String startTime, int seed) {
//		 int actualSpeed = obsSpeed * acceration;
//		 
//	}
	
	// The simulated sensor list does not include original sensor name
	public static List<String> scaleSensorIds(List<String> sensorIds, int scaleNum, String simulatedName) {
		List<String> simulatedSensorIds = new ArrayList<String>();;
		int oriSensorSize = sensorIds.size();
		int simulatedSize = oriSensorSize * scaleNum;
		
		for (int i = 1; i <= simulatedSize; ++i) {
			simulatedSensorIds.add(simulatedName + i);
		}
		
		return simulatedSensorIds;
	}
	
	// Scale the data and write the simulated data to a new file
	public static void simulateTempObsData(List<String> sensorIds, String sensorType, List<Integer> payloadLimits, 
			int obsSpeed, String startTime, int extendDays,  int seed, String fileName) {
		Random rand = new Random(seed); // set up seed for random
		int sensorSize = sensorIds.size();
		
		// write data to file
		JsonWriter jsonWriter = null;
    	try {
		  jsonWriter = new JsonWriter(new FileWriter(fileName));
		  jsonWriter.setIndent("  ");
		  jsonWriter.beginArray();
		  
		  for (int k = 0; k < extendDays; ++k) {
			  String timestamp = timeAddDays(startTime, k);
			  for (int i = 0; i < obsSpeed; ++i) {  
				  for (int j = 0; j < sensorSize; ++j) {
					  // get random temperature
					  int payloadTemp = rand.nextInt(payloadLimits.get(1)-payloadLimits.get(0)+1)+payloadLimits.get(0);
					  jsonWriter.beginObject();
					  jsonWriter.name("typeId");
					  jsonWriter.value(sensorType);
					  jsonWriter.name("timestamp");
					  jsonWriter.value(timestamp);
					  jsonWriter.name("payload");
					  jsonWriter.beginObject();
					  jsonWriter.name("temperature");
					  jsonWriter.value(payloadTemp);
					  jsonWriter.endObject();
					  jsonWriter.name("sensorId");
					  jsonWriter.value(sensorIds.get(j));
					  jsonWriter.endObject();
				  }
				  
				  timestamp = increaseTime(timestamp, obsSpeed); // increase time based on the observation speed
			  }
		  }
    	  jsonWriter.endArray(); // close the JSON array
    	  
    	} catch (IOException e) {
    	  System.out.println("IO error");
    	}finally{
    	  try {
    	      jsonWriter.close();
    	  } catch (IOException e) {
    	  	 System.out.println("IO error");
    	  }
    	}
	}
	
	// Increase timestamp for each observation based on observation speed
	public static String increaseTime(String startTime, int obsSpeed) {
		Timestamp ts = Timestamp.valueOf(startTime);
		long time = ts.getTime();
		time += (float)24*3600*1000/obsSpeed + 0.5*1000; // < 0.5s round down and > 0.5s round up
		
		ts.setTime(time);
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String timeStr  = dateFormat.format(ts);
		return timeStr;
	}
	
	// Parse the original file with GSON and update with new contents
	public static void createNewFileWithUpdatedContent(int seed, List<Integer> payloadLimits, int extendDays) throws FileNotFoundException {
		Random rand = new Random(seed); // set up seed for random
		
		// Read and parse the JSON file with GSON 
		Gson gson = new Gson();
		JsonReader reader = new JsonReader(new FileReader("POST/temperatureObs.json"));
        Obs[] data = gson.fromJson(reader, Obs[].class); // contains the whole reviews list
        
        // Write the updated information to new file
        JsonWriter jsonWriter = null;
    	try {
		  jsonWriter = new JsonWriter(new FileWriter("SimulatedData/simulatedTempObs.json"));
		  jsonWriter.setIndent("  ");
		  jsonWriter.beginArray();
		  for (int i = 0; i < data.length; ++i) {
			  // get random temperature
			  int payloadTemp = rand.nextInt(payloadLimits.get(1)-payloadLimits.get(0)+1)+payloadLimits.get(0);
			  String timestamp = timeAddDays(data[i].getTimestamp(), 1);
			  jsonWriter.beginObject();
			  jsonWriter.name("typeId");
			  jsonWriter.value(data[i].getTypeId());
			  jsonWriter.name("timestamp");
			  jsonWriter.value(timestamp);
			  jsonWriter.name("payload");
			  jsonWriter.beginObject();
			  jsonWriter.name("temperature");
			  jsonWriter.value(payloadTemp);
			  jsonWriter.endObject();
			  jsonWriter.name("sensorId");
			  jsonWriter.value(data[i].getSensorId());
			  jsonWriter.endObject();
		  }
		  
    	  jsonWriter.endArray(); // close the json array
    	} catch (IOException e) {
    	  System.out.println("IO error");
    	}finally{
    	  try {
    	      jsonWriter.close();
    	  } catch (IOException e) {
    	  	 System.out.println("IO error");
    	  }
    	}
	}
	
	// Add days to timestamp string
	public static String timeAddDays(String timestamp, int days) {
		Timestamp ts = Timestamp.valueOf(timestamp);
		long time = ts.getTime();
		time += 24*3600*1000*days;
		
		ts.setTime(time);
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String string  = dateFormat.format(ts);
		return string;
	}
	
	// Copy the original JSON file content into a new file
	public static void copyFile() {
		InputStream inStream = null;
		OutputStream outStream = null;

    	try{
    	    File originalfile =new File("POST/temperatureObs.json");
    	    File newfile =new File("SimulatedData/simulatedTempObs.json");

    	    inStream = new FileInputStream(originalfile);
    	    outStream = new FileOutputStream(newfile);

    	    byte[] buffer = new byte[1024];

    	    int length;
    	    //copy the file content in bytes
    	    while ((length = inStream.read(buffer)) > 0){
    	    	outStream.write(buffer, 0, length);
    	    }

    	    inStream.close();
    	    outStream.close();

    	    System.out.println("File is copied successful!");

    	}catch(IOException e){
    		e.printStackTrace();
    	}
    	
    	
	}
}

// Obs read class for GSON
class Obs{
	private String typeId;
	private String timestamp;
	private JSONObject payload;
	private String sensorId;
	
	// constructor
	public Obs() {
	}
	
	// return the sensor type id
	public String getTypeId() {
		return this.typeId;
	}
	
	// return the timestamp string
	public String getTimestamp() {
		return this.timestamp;
	}
	
	// return the payload JSONObject
	public JSONObject getPayload() {
		return this.payload;
	}
	
	// return the sensor Id
	public String getSensorId() {
		return this.sensorId;
	}
	
	// update the timestamp
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
}

// Temperature observation parsing class
class TemperatureObs 
{
	private List<String> typeIds;
	private List<String> sensorIds;
	private List<String> timestamps;
	private List<Integer> payloads;
	private List<Integer> payloadLimits;
	private int obsCount; // total number of observation
	private int obsSpeed; // observation count per day and per sensor
	
	public TemperatureObs() {
		this.typeIds = new ArrayList<String>();
		this.sensorIds = new ArrayList<String>();
		this.timestamps = new ArrayList<String>();
		this.payloads = new ArrayList<Integer>();
		this.payloadLimits = new ArrayList<Integer>(2);
		this.obsCount = 0;
		this.obsSpeed = 0;
	}
	
	public void parseData() {
		// parse json file
        JSONArray observations = new JSONArray();
        try {
            JSONParser parser = new JSONParser();
            observations = (JSONArray) parser.parse(new FileReader("POST/temperatureObs.json"));
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
            this.obsCount++;        // count the total observations
            
            // get sensor type IDs
            String typeId = (String) observation.get("typeId");
            if (!this.typeIds.contains(typeId)) {
				this.typeIds.add(typeId);
			}

			// get sensor ids
			String sensorId = (String) observation.get("sensorId");
            if (!this.sensorIds.contains(sensorId)) {
				this.sensorIds.add(sensorId);
			}
			
			// get timestamps
			String timestamp = (String) observation.get("timestamp");
            if (!this.timestamps.contains(timestamp)) {
                this.timestamps.add(timestamp);
            }
            
			// get sensor observation number per day
			Timestamp currTime = Timestamp.valueOf(timestamp);
			Timestamp startTime = Timestamp.valueOf("2017-07-11 00:00:00");
			Timestamp endTime = Timestamp.valueOf("2017-07-12 00:00:00");
			if ((currTime.equals(startTime) || currTime.after(startTime))
				&& currTime.before(endTime) && sensorId.equals(this.sensorIds.get(0))) {
				this.obsSpeed++;
			}

			// get payload
            JSONObject payloadObj = (JSONObject) observation.get("payload");
            Integer payload = (int) (long) payloadObj.get("temperature");
            if (!this.payloads.contains(payload)) {
            	this.payloads.add(payload);
            } 
            
		}
	}
	
	// return type Ids
	public List<String> getTypeIds() {
		return this.typeIds;
	}
	
	// return sensor Ids
	public List<String> getSensorIds() {
		return this.sensorIds;
	}
	
	// return the bottom and up limits of payload
	public List<Integer> getPayloadLimits() {
		int minTemp = this.payloads.get(0);
		int maxTemp = minTemp;
		int payloadSize = this.payloads.size();

		for (int i = 0; i < payloadSize; ++i) {
			if (this.payloads.get(i) < minTemp) {
				minTemp = this.payloads.get(i);
			}

			if (this.payloads.get(i) > maxTemp) {
				maxTemp = this.payloads.get(i);
			}
		}

		this.payloadLimits.add(minTemp);
		this.payloadLimits.add(maxTemp);
		return this.payloadLimits;
	}
	
	// return the observation number per day per sensor
	public int getObsSpeed() {
		return this.obsSpeed;
	}
	
	// return the timestamps
	public List<String> getTimestamps() {
		return this.timestamps;
	}
	
	// return the recorded Days of current observations
	public int getRecordDays() {
		return this.obsCount/(this.sensorIds.size() * this.obsSpeed);
	}
}


