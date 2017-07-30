// Author: Fangfang Fu
// Date: 7/15/2017

// javac -cp json-simple-1.1.1.jar Query.java Sensor.java
// java -classpath .;json-simple-1.1.1.jar Query
// . = current directory
// ; is for seperating entries in the list
// one;two;three
// .. = previous directory
// ..\.. = up 2 directories

// query type 1 
// Select_Sensor(X) Query
// select * from sensor where id = X


import java.util.*;

public class Query
{
    public static void main (String[] args) {
    	QueryGenerator query = new QueryGenerator();
    	
        // parse sensor file
        Sensor sensors = new Sensor();
        sensors.parseData();
        List<String> sensorIds = sensors.getIds();
        // List<String> sensorTypeIds = sensors.getTypeIds();
        List<String> locationIds = sensors.getInfraStructureIds();
        
        // parse sensorType file
        SensorType sensorTypes = new SensorType();
        sensorTypes.parseData();
        List<String> sensorTypeIds = sensorTypes.getIds();
   
        // parse observation file
        Observation observations = new Observation();
        observations.parseData();
        // List<String> timestamps = observations.getTimestamps();
        
        // parse temperatureObs file
        TemperatureObs temperatureObs = new TemperatureObs();
        temperatureObs.parseData();
        // List<String> typeIds = temperatureObs.getTypeIds();
        List<String> timestamps = temperatureObs.getTimestamps();
        List<Integer> payloads = temperatureObs.getPayloads();
        
        // parse user file
        User users = new User();
        users.parseData();
        List<String> userIds = users.getIds();
        
        // parse infrastructureType file
        InfrastructureType infrastructureTypes = new InfrastructureType();
        infrastructureTypes.parseData();
        List<String> locationTypes = infrastructureTypes.getIds();
        
        // Select_Sensor(X):  Select a sensor with id X
        int selectSensorNum = 3; // < 82 sensorId number
        int selectSensorseed = 1;
        query.SelectSensorGenerator(sensorIds, selectSensorseed, selectSensorNum);

        // Space_to_Sensor(X, {locations}): List all sensor of type X that can observe Locations in {locations}.
        System.out.println();
        int spaceSensorNum = 3; // <= 6 sensor Types
        int spaceSensorSeed = 1;
        query.SpaceToSensorGenerator(sensorTypeIds, locationIds, spaceSensorSeed, spaceSensorNum);

        // Observations(X, <T1, T2>): Select Observations From a Sensor with id X between time range T1 and T2.
        System.out.println();  // <= 82 sensorIds number
        int observeNum1 = 3;
        int observeSeed1 = 1;
        query.ObservationOfSingleSensorGenerator(sensorIds, timestamps, observeSeed1, observeNum1);

        // Observations({X1,X2, ...}, <T1, T2>): Select Observations From Sensors in list {X1, X2 ...} between time range T1 and T2.
        System.out.println(); 
        int observeNum2 = 3;   // <= 2^82 (did not check duplicate because it has a very small possibility to have duplicate
        int observeSeed2 = 1;
        query.ObservationOfMultipleSensorGenerator(sensorIds, timestamps, observeSeed2, observeNum2);

        /* Observations(X,<T1,T2>, Y, <Y_a, Y_b>): Select Observations of Sensors of type X between time range T1 and T2 
           and payload.Y in range (Y_a, Y_b) */
        System.out.println(); 
        int observeNum3 = 3;   // <= 6 sensor Types
        int observeSeed3 = 1;
        query.ObservationOfSensorTypeGenerator(sensorTypeIds, timestamps, payloads, observeSeed3, observeNum3);
        
        /* Statistics({sensors}, <begin-date, end-date> ): Average number of observations per day between the begin 
         * and end dates for each sensor in {sensors} 
         */
        System.out.println(); 
        int statisticsNum = 3; 
        int statisticSeed = 1;
        query.StatisticsGenerator(sensorIds, statisticSeed, statisticsNum);
        
        // Trajectories(date, loc1, loc2): Fetch names of users who went from Location loc1 to Location loc2  on the specified  date.
        System.out.println(); 
        int trajectoriesNum = 3;
        int trajectoriesSeed = 1;
        query.TrajectoriesGenerator(locationIds, trajectoriesSeed, trajectoriesNum);
        
        // Colocate(X, date): Select all users who were in the same Location as User X on a specified date.
        System.out.println(); 
        int colocateNum = 3;  // < 14 userId number
        int colocateSeed = 1;
        query.ColocateGenerator(userIds, colocateSeed, colocateNum);
        
        // Time_Spent(X, Y) Fetch average time spent per day by User X in Locations of Type Y.
        System.out.println(); 
        int timeSpentNum = 3; // < 14 userId number
        int timeSpentSeed = 1;
        query.TimeSpentGenerator(userIds, locationTypes, timeSpentSeed, timeSpentNum);
        
        // Occupancy({locations}, time-unit, <begin-time, end-time>): occupancy as a function of time between begin-time and end-time
        System.out.println(); 
        int occupancyNum = 3; 
        int occupancySeed = 1;
        query.OccupancyGenerator(locationIds, timestamps, occupancySeed, occupancyNum);
    }
    
}
