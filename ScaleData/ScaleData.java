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
		copyFile("temperatureObs.json");

        // temporal scaling -- where we extend the data from same sensors for a long time
		int extendDays = 3;
		double timeScaleNoise = 0.25;
		String filename1 = "simulatedTempObs1.json";
		timeScale(timeScaleNoise, extendDays, filename1);
		
		// speed scaling -- where same devices generate data, but at a faster speed
		int speedScaleNum = 2;
		double speedScaleNoise = 0.25;
		String outputFilename = "simulatedTempObs2.json";
		speedScale(speedScaleNum, speedScaleNoise, outputFilename);
		
		// device scaling -- when we scale number of devices
		int deviceScaleNum = 10;
		String simulatedName = "simulatedEmeter"; 
		double deviceScaleNoise = 0.25;
		String outputfilename3 = "simulatedTempObs3.json";
		deviceScale(deviceScaleNum, deviceScaleNoise, simulatedName, outputfilename3);
		
		System.out.println("done");
		// createNewFileWithUpdatedContent(seed, payloadLimits, extendDays);
	}
	
	// device scaling -- when we scale number of devices
	public static void deviceScale(int scaleNum, double deviceScaleNoise, String simulatedName, String outputFilename) throws FileNotFoundException {
		// parse temperatureObs file
		TemperatureObs temperatureObs = new TemperatureObs();
		temperatureObs.parseData("simulatedObs.json");
		List<String> sensorTypeIds = temperatureObs.getTypeIds();
		String sensorType = sensorTypeIds.get(0);
		List<String> sensorIds = temperatureObs.getSensorIds();
		List<Integer> payloads = temperatureObs.getPayloads();
        List<String> timestamps = temperatureObs.getTimestamps();
        int recordDays = temperatureObs.getRecordDays();
        int obsSpeed = temperatureObs.getObsSpeed();
        
		Random rand = new Random(); // set up random seed
		int sensorSize = sensorIds.size();
		int scaledSensorSize = sensorSize * scaleNum;
		sensorIds = scaleSensorIds(sensorIds, scaleNum, simulatedName);
		
		// write data to file
        JsonWriter jsonWriter = null;
    	try {
		  jsonWriter = new JsonWriter(new FileWriter("SimulatedData/" + outputFilename));
		  jsonWriter.setIndent("  ");
		  jsonWriter.beginArray();
		
		  for (int m = 0; m < recordDays; ++m) {
			  int pastObs = m * obsSpeed * sensorSize;
			  for (int i = 0; i < obsSpeed; ++i) {
				  String timestamp = timestamps.get(i);
				  for (int j = 0; j < scaledSensorSize; ++j) {
					  int payload = 0;
					  if (j < sensorSize) {
						  payload = payloads.get(pastObs+i*sensorSize+j);
					  } else {
						  int n = rand.nextInt(sensorSize);
						  payload = getRandAroundPayload(payloads.get(pastObs+i*sensorSize+n), deviceScaleNoise);
					  }

					  jsonWriter = helpWriteToFile(jsonWriter, sensorType, timestamp, payload, sensorIds.get(j));
				  }
			  }	  
		  }
		  
		  jsonWriter.endArray(); // close the JSON array 
    	} catch (IOException e) {
    	  System.out.println("IO error");
    	} finally {
    	  try {
    	      jsonWriter.close();
    	  } catch (IOException e) {
    	  	 System.out.println("IO error");
    	  }
    	}
		  
	}

	
	// Temporal scaling -- where we extend the data from same sensors for a long time
	public static void timeScale(double timeScaleNoise, int extendDays, String outputFilename) throws FileNotFoundException {
		// parse temperatureObs file
		TemperatureObs temperatureObs = new TemperatureObs();
		temperatureObs.parseData("simulatedObs.json");
		List<String> sensorTypeIds = temperatureObs.getTypeIds();
		String sensorType = sensorTypeIds.get(0);
		List<String> sensorIds = temperatureObs.getSensorIds();
		List<Integer> payloads = temperatureObs.getPayloads();
        List<String> timestamps = temperatureObs.getTimestamps();
        int obsSpeed = temperatureObs.getObsSpeed();
        int recordDays = temperatureObs.getRecordDays();
		int sensorSize = sensorIds.size();
        
        
        // write data to file
        JsonWriter jsonWriter = null;
    	try {
		  jsonWriter = new JsonWriter(new FileWriter("SimulatedData/" + outputFilename));
		  jsonWriter.setIndent("  ");
		  jsonWriter.beginArray();
		  
		  // original observations
		  for (int m = 0; m < recordDays; ++m) {
			  int pastObs = m * obsSpeed * sensorSize;
			  for (int i = 0; i < obsSpeed; ++i) { 
				  String timestamp = timestamps.get(i);
				  for (int j = 0; j < sensorSize; ++j) {
					  int payload = payloads.get(pastObs+i*sensorSize+j);
					  jsonWriter = helpWriteToFile(jsonWriter, sensorType, timestamp, payload, sensorIds.get(j));
				  }
			  }
		  }
		  
		  // extend days' observations
		  for (int m = 0; m < extendDays; ++m) {
			  int pastDays = recordDays + m;
			  for (int i = 0; i < obsSpeed; ++i) {
				  String timestamp = timeAddDays(timestamps.get(i), pastDays);
				  for (int j = 0; j < sensorSize; ++j) {
					  int payload = getRandAroundPayload(payloads.get(i*sensorSize+j), timeScaleNoise);
					  jsonWriter = helpWriteToFile(jsonWriter, sensorType, timestamp, payload, sensorIds.get(j));
				  }
				  timestamp = increaseTime(timestamp, obsSpeed); // increase time based on the observation speed
			  }
		  }
		  
		  jsonWriter.endArray(); // close the JSON array
    	  
    	} catch (IOException e) {
    	  System.out.println("IO error");
    	} finally {
    	  try {
    	      jsonWriter.close();
    	  } catch (IOException e) {
    	  	 System.out.println("IO error");
    	  }
    	}
		  
	}
	
	// Speed scaling -- where same devices generate data, but at a faster speed: keep original observations 
    // and add more between two nearby timestamps for each sensor
	public static void speedScale(int speedScaleNum, double speedScaleNoise, String outputFilename) throws FileNotFoundException {
		// parse temperatureObs file
		TemperatureObs temperatureObs = new TemperatureObs();
		temperatureObs.parseData("simulatedObs.json");
		List<String> sensorTypeIds = temperatureObs.getTypeIds();
		String sensorType = sensorTypeIds.get(0);
		List<String> sensorIds = temperatureObs.getSensorIds();
		List<Integer> payloads = temperatureObs.getPayloads();
        List<String> timestamps = temperatureObs.getTimestamps();
        int recordDays = temperatureObs.getRecordDays();
        int obsSpeed = temperatureObs.getObsSpeed();
        
        int sensorSize = sensorIds.size();
        int scaleSpeed = obsSpeed * speedScaleNum;
        
        // write data to file
        JsonWriter jsonWriter = null;
    	try {
		  jsonWriter = new JsonWriter(new FileWriter("SimulatedData/" + outputFilename));
		  jsonWriter.setIndent("  ");
		  jsonWriter.beginArray();
		  for (int m = 0; m < recordDays; ++m) {
			  String timestamp = timeAddDays(timestamps.get(0), m);
			  for (int i = 0; i < obsSpeed-1; ++i) {  
				  // original observations
				  for (int j = 0; j < sensorSize; ++j) {
					  int payload = payloads.get(j+i*sensorSize);
					  jsonWriter = helpWriteToFile(jsonWriter, sensorType, timestamp, payload, sensorIds.get(j));
				  }
				  
				  timestamp = increaseTime(timestamp, scaleSpeed); // increase time based on the observation speed
				  
				  // add simulated observations between two observations for each sensor
				  for (int k = 0; k < speedScaleNum - 1; ++k) {
					  for (int j = 0; j < sensorSize; ++j) {
						  // get random temperature
						  int payload = getRandBetweenPayloads(payloads.get(i*sensorSize+j), payloads.get(i*sensorSize+j+sensorSize), speedScaleNoise);
						  jsonWriter = helpWriteToFile(jsonWriter, sensorType, timestamp, payload, sensorIds.get(j));
					  }
					  timestamp = increaseTime(timestamp, scaleSpeed); // increase time based on the observation speed
				  }  
			  }
			  
			  // handle the end timestamps for all sensors
			  for (int j = 0; j < sensorSize; ++j) {
				  int payload = payloads.get((obsSpeed-1)*sensorSize + j); 
				  jsonWriter = helpWriteToFile(jsonWriter, sensorType, timestamp, payload, sensorIds.get(j));
			  }
		  }
    	  jsonWriter.endArray(); // close the JSON array
    	  
    	} catch (IOException e) {
    	  System.out.println("IO error");
    	} finally {
    	  try {
    	      jsonWriter.close();
    	  } catch (IOException e) {
    	  	 System.out.println("IO error");
    	  }
    	}
	}
	
	public static JsonWriter helpWriteToFile(JsonWriter jsonWriter, String sensorType, String timestamp, int payload, String sensorId) {
		try {
			jsonWriter.beginObject();
			jsonWriter.name("typeId");
			jsonWriter.value(sensorType);
			jsonWriter.name("timestamp");
			jsonWriter.value(timestamp);
			jsonWriter.name("payload");
			jsonWriter.beginObject();
			jsonWriter.name("temperature");
			jsonWriter.value(payload);
			jsonWriter.endObject();
			jsonWriter.name("sensorId");
			jsonWriter.value(sensorId);
			jsonWriter.endObject();
		}  catch (IOException e) {
			System.out.println (e.toString());
			System.out.println("IO error");
	    	  
		}

		return jsonWriter;
	}
	
	
	// Get random payload 
	public static int getRandAroundPayload(int payload, double scaleNoise) {
		Random rand = new Random(); // set up random seed
		int min = (int) (payload * (1 - scaleNoise));
		int max = (int) (payload * (1 + scaleNoise));
		return rand.nextInt(max-min+1) + min;
	}
	
	// Generate random number between two real payloads within certain percent of range
	public static int getRandBetweenPayloads(int payload1, int payload2, double scaleNoise) {
		Random rand = new Random();
		if (payload1 < payload2) {
			int min = (int) (payload1 * (1 - scaleNoise));
			int max = (int) (payload2 * (1 + scaleNoise));
			return rand.nextInt(max-min+1) + min;
		} else {
			int min = (int) (payload2 * (1 - scaleNoise));
			int max = (int) (payload2 * (1 + scaleNoise));
			return rand.nextInt(max-min+1) + min;
		}
	}
	
	// Increase timestamp for each observation based on observation speed
	public static String increaseTimestamp(String startTime, int obsSpeed) {
		Timestamp ts = Timestamp.valueOf(startTime);
		long time = ts.getTime();
		time += (float)24*3600*1000/obsSpeed + 0.5*1000; // < 0.5s round down and > 0.5s round up
		
		ts.setTime(time);
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String timeStr  = dateFormat.format(ts);
		return timeStr;
	}
		
		
	// The simulated sensor list does not include original sensor name
	public static List<String> scaleSensorIds(List<String> sensorIds, int scaleNum, String simulatedName) {
		List<String> scaledSensorIds = new ArrayList<String>();
		int oriSensorSize = sensorIds.size();
		int simulatedSize = oriSensorSize * scaleNum;
		
		for (int i = 0; i < oriSensorSize; ++i) {
			scaledSensorIds.add(sensorIds.get(i));
		}
		
		for (int i = 1; i <= simulatedSize - oriSensorSize; ++i) {
			scaledSensorIds.add(simulatedName + i);
		}
		
		return scaledSensorIds;
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
    		System.out.println (e.toString());
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
		// Random rand = new Random(seed); // set up seed for random
		
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
			  // int payloadTemp = rand.nextInt(payloadLimits.get(1)-payloadLimits.get(0)+1)+payloadLimits.get(0);
			  String timestamp = timeAddDays(data[i].getTimestamp(), 1);

			  jsonWriter.beginObject();
			  jsonWriter.name("typeId");
			  jsonWriter.value(data[i].getTypeId());
			  jsonWriter.name("timestamp");
			  jsonWriter.value(timestamp);
			  jsonWriter.name("payload");
			  jsonWriter.beginObject();
			  jsonWriter.name("temperature");
			  jsonWriter.value(data[i].getPayloadValue("temperature"));
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
	public static void copyFile(String filename) {
		InputStream inStream = null;
		OutputStream outStream = null;

    	try{
    	    File originalfile =new File("POST/" + filename);
    	    File newfile =new File("SimulatedData/simulatedObs.json");

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
	
	public double getPayloadValue(String payloadName) {
		return (double) this.payload.get(payloadName);
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
	
	public void parseData(String filename) {
		// parse json file
        JSONArray observations = new JSONArray();
        try {
            JSONParser parser = new JSONParser();
            observations = (JSONArray) parser.parse(new FileReader("SimulatedData/" + filename));
        } catch (FileNotFoundException ex) {
        	System.out.println (ex.toString());
            System.err.println("File not found");
        } catch (IOException ex) {
        	System.out.println (ex.toString());
            System.err.println("Input or out error");
        } catch (ParseException ex) {
        	System.out.println (ex.toString());
            System.err.println("Parse error");
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
			Timestamp startTime = Timestamp.valueOf("2017-07-11 00:00:00"); //TODO
			Timestamp endTime = Timestamp.valueOf("2017-07-12 00:00:00"); //TODO:
			if ((currTime.equals(startTime) || currTime.after(startTime))
				&& currTime.before(endTime) && sensorId.equals(this.sensorIds.get(0))) {
				this.obsSpeed++;
			}

			// get payload
            JSONObject payloadObj = (JSONObject) observation.get("payload");
            Integer payload = (int) (long) payloadObj.get("temperature");
            this.payloads.add(payload);      
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
	
	public List<Integer> getPayloads() {
		return this.payloads;
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

		this.payloadLimits.add(minTemp); // first element is the smallest
		this.payloadLimits.add(maxTemp); // second element is the largest
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


