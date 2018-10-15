import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;


/**
 * @author Ankita,Ushang,Xia
 */
public class AverageMonthlyDelay {

    public static class AverageMonthlyDelayMapper extends Mapper<Object, Text, Text, FlightDataWritable> {

        // initialize CSVParser as comma separated values
        private CSVParser csvParser = new CSVParser(',', '"');
        private static HashSet<String> topAirlines = new HashSet<>();
        private static HashSet<String> topAirports = new HashSet<>();
        private static HashMap<String,FlightDataWritable> airportMonthData = new HashMap<>();
        private static HashMap<String,FlightDataWritable> airlineMonthData = new HashMap<>();

        /**
         * The setup method is called only once during each Mapper before the execution of the first call to map(). Thus
         * we read the output of the first MR phase 1, calculate the top 5 and store it in a HashSet
         * @param context       The context of this jobs configuration
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //Read the previous file and call the helper function to get top 5
            readFile("/part-r-00000",topAirlines,context);
            readFile("/part-r-00001",topAirports,context);
        }

        /**
         * Helper function that read a file by the given name from HDFS and then creates a HashSet of only the top 5
         * airlines or airport
         * @param fileName  The file name written in the first MR phase. It is either part-r-00000 for Airlines and
         *                  part-r-00001 for airports
         * @param target    The HashSet in which the result needs to be stored
         * @param context   The context of this jobs configuration
         * @throws IOException
         */
        public static void readFile(String fileName, HashSet<String> target, Context context) throws IOException{
            Map<String,Integer> t = new HashMap<>();
            String phase1OutputPath = context.getConfiguration().get("phase1output");
            Path pt = new Path(phase1OutputPath+fileName);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            try {
                String line;
                line = br.readLine();
                while (line != null) {
                    String keyValue[] = line.split("\\t");  // Since the reducer writes with a tab DELIMITER
                    t.put(keyValue[0].substring(3),Integer.parseInt(keyValue[1]));  //remove the "AL_" or "AP_"
                    line = br.readLine();
                }
            } finally {
                br.close();
            }
            // Iterate and get the top 5
            t = sortByValue(t);
            Iterator i = t.entrySet().iterator();
            int count=0;
            while(i.hasNext() && count<5){
                Map.Entry pair = (Map.Entry)i.next();
                target.add(pair.getKey().toString());
                count++;
            }
        }

        /**
         * This function using Java 8 features to sort a hashmap by its value in descending order
         * @author Stack Overflow
         * @param map   The map which needs to be sorted
         * @return map  The map sorted by its values in descending  order
         */
        public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
            return map.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue,
                            (e1, e2) -> e1, LinkedHashMap::new));
        }

        /**
         * The map function reads each CSV line preprocess it to see if passes the sanity test and then adds it to the
         * apporpriate hashmap of airportMonthlyData or airlineMonthlyData or both based on the record and the top 5
         * airports and airlines
         * @param key       Long offset which we can ignore
         * @param value     Each line of the CSV input file
         * @param context   The context of this jobs configuration
         * @throws IOException  If the file cannot be read/found
         * @throws InterruptedException
         */
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] inVal = csvParser.parseLine(value.toString());
            if(isRecordValidAndRequired(inVal)){
                try{
                    // find out if its cancelled or not
                    if(Integer.parseInt(inVal[47]) == 0){       //Flight is not cancelled
                        // Calculate normalized delay
                        if(isNotCancelled(inVal)){
                            double delay = 0;

                            if(Double.parseDouble(inVal[42]) > 0)
                                delay = Double.parseDouble(inVal[42]);

                            double normalizedDelay = delay / Integer.parseInt(inVal[50]);

                            if(topAirlines.contains(inVal[8]))
                                addToHashmap(airlineMonthData,inVal,"AL",normalizedDelay);

                            if(topAirports.contains(inVal[23]))
                                addToHashmap(airportMonthData,inVal,"AP",normalizedDelay);
                        }
                        else
                            return;
                    }
                    else if(Integer.parseInt(inVal[47]) == 1){  //Flight is cancelled
                        if(topAirlines.contains(inVal[8]))
                            addToHashmap(airlineMonthData,inVal,"AL",4.0);

                        if(topAirports.contains(inVal[23]))
                            addToHashmap(airportMonthData,inVal,"AP",4.0);
                    }
                    else
                        return; // Corrupt data for CANCELLED FIELD
                }
                catch(Exception e){
                    return;
                }
            }
            else{
                return;
            }
        }

        /**
         * This helper function adds the current record to the appropriate hashmap
         * @param target            The target hashmap in which the record needs to be added
         * @param record            The record that needs to be added
         * @param type              Type is either "AL" for airlines or "AP" for airports
         * @param normalizedDelay   The normalized delay of this record
         */
        public static void addToHashmap(HashMap<String,FlightDataWritable> target,  String[] record,String type, double normalizedDelay){
                FlightDataWritable t;
                String hashmapKey = "";
                if(type.equals("AL")){
                    hashmapKey = type + "_" + record[8] + "_" + record[23] + "_" + record[2] + "_" + record[0];
                    target = airlineMonthData;
                }
                else {
                    hashmapKey = type + "_" + record[23] + "_" + record[8] + "_" + record[2] + "_" + record[0];
                    target = airportMonthData;
                }
                if(target.containsKey(hashmapKey)){
                    t = target.get(hashmapKey);
                    double newVal = t.getNormalizedDelay().get() + normalizedDelay;
                    t.setNormalizedDelay(new DoubleWritable(newVal));
                    int noOfFlights = t.getFlights().get() + 1;
                    t.setFlights(new IntWritable(noOfFlights));
                }
                else{
                    t = new FlightDataWritable();
                    t.setFlights(new IntWritable(1));
                    t.setNormalizedDelay(new DoubleWritable(normalizedDelay));
                }
                target.put(hashmapKey,t);
            }

        /**
         * This helper function does the sanity check for the passed record that is not cancelled
         * @param record    The not cancelled record which needs to be sanity tested
         * @return boolean  True if the sanity test passes, false otherwise
         */
        public static boolean isNotCancelled(String[] record){
            try{
                //Test whether ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
                if(Integer.parseInt(record[41])>0 && Integer.parseInt(record[30])>0 &&
                        Integer.parseInt(record[51])>0 && Double.parseDouble(record[42])>Integer.MIN_VALUE){
                    // timeZone = CRSArrTime - CRSDepTime - CRSElapsedTime;
                    int timeZone = Integer.parseInt(record[40]) - Integer.parseInt(record[29]) - Integer.parseInt(record[50]);
                    int res = Integer.parseInt(record[41]) - Integer.parseInt(record[30]) - Integer.parseInt(record[51]) - timeZone;
                    if(res == 0)
                        return true;
                    else
                        return false;
                }
            }
            catch(Exception e){
                return false;
            }
            return false;
        }

        /**
         * Checks if the record is required(non empty) and valid(non zero and number
         * @param record The record that needs to be checked
         * @return boolean True if the record is valid & required else false
         */
        public static boolean isRecordValidAndRequired(String[] record){

            if (record == null || record.length == 0) //If row is empty don't do anything
                return false;

            if(checkIfNonZero(record) && checkIfNotEmpty(record) && timezoneCheck(record)) //Now check its validity
                return true;
            return false;
        }

        /**
         * Checks if the timezone is proper ie basically checks timezone%60 is 0
         * @param record The record that needs to be checked
         * @return boolean True if the timezone in the record is valid
         */
        public static boolean timezoneCheck(String[] record){
            try {
                // timezone = CRS_ARR_TIME - CRS_DEP_TIME - CRS_ELAPSED_TIME
                int timeZone = Integer.parseInt(record[40]) - Integer.parseInt(record[29]) - Integer.parseInt(record[50]);
                if(timeZone % 60 == 0)
                    return true;
            }
            catch(Exception e){
                return false;
            }
            return false;
        }

        /**
         * Checks if the fields(Origin,Destination,CityName,State,StateName) of the record are not empty
         * @param record The record that needs to be checked
         * @return boolean True if the fields are nonempty else false
         */
        public static boolean checkIfNotEmpty(String[] record){
            //Origin - 14, Destination- 23,  CityName - 15 & 24, State - 16 & 25, StateName - 18 & 27 should not be empty
            if(record[14].isEmpty() || record[23].isEmpty() || record[15].isEmpty() || record[24].isEmpty() ||
                    record[16].isEmpty() || record[25].isEmpty() || record[18].isEmpty() || record[27].isEmpty() ||
                    record[2].isEmpty())
                return false;
            return true;
        }

        /**
         * Checks if the fields(AirportID,AirportSeqID,CityMarketID,StateFips,Wac,CRS_ARR_TIME,CRS_DEP_TIME) are non zero
         * and the validty of month and year field
         * @param record The record that needs to be checked
         * @return boolean True if the fields are nonzero else false
         */
        public static boolean checkIfNonZero(String[] record){
            try{
                /**AirportID - 11 & 20,  AirportSeqID - 12 & 21, CityMarketID - 13 & 22, StateFips - 17 & 26 , Wac - 19 & 28
                 * should be larger than 0, CRS_ARR_TIME - 40 & CRS_DEP_TIME - 29 should not be 0
                 **/
                if(Integer.parseInt(record[11])>0 && Integer.parseInt(record[20])>0 &&
                        Integer.parseInt(record[12])>0 && Integer.parseInt(record[21])>0 &&
                        Integer.parseInt(record[13])>0 && Integer.parseInt(record[22])>0 &&
                        Integer.parseInt(record[17])>0 && Integer.parseInt(record[26])>0 &&
                        Integer.parseInt(record[19])>0 && Integer.parseInt(record[28])>0 &&
                        Integer.parseInt(record[40])!=0 && Integer.parseInt(record[29])!=0 &&
                        Integer.parseInt(record[2])>0 && Integer.parseInt(record[2])<13 &&   // Check for month correctness
                        Integer.parseInt(record[0])>=1989 && Integer.parseInt(record[0])<=2017){  // Check for year correctness
                    return true;
                }
            }
            catch(Exception e){
                return false;
            }
            return false;
        }

        /**
         * The cleanup method is called only once per Mapper. It is called after the last call to the map() is done.Thus
         * we send the hashmap to the reducer by context writing each line in this function.
         * @param context               The context of this jobs configuration
         * @throws IOException          If the exceptions is produced by failed or interrupted I/O operations.
         * @throws InterruptedException
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Iterator it = airlineMonthData.entrySet().iterator();
            Iterator it2 = airportMonthData.entrySet().iterator();
            while(it.hasNext()){
                Map.Entry pair = (Map.Entry)it.next();
                context.write(new Text(String.valueOf(pair.getKey())),(FlightDataWritable)pair.getValue());
            }
            while(it2.hasNext()){
                Map.Entry pair = (Map.Entry)it2.next();
                context.write(new Text(String.valueOf(pair.getKey())),(FlightDataWritable) pair.getValue());
            }
        }
    }


    public static class AverageMonthlyDelayReducer extends Reducer<Text, FlightDataWritable, Text, DoubleWritable> {
        /**
         * The reducer aggregates all the normalized delays and the total flights sent by the mapper for each key and
         * finds the mean normalized delay for the same
         * @param key       The key sent by the mapper. It will be in the form of AL_<AIRLINE>_<AIRPORT>_<MONTH>_<YEAR>
         *                  or AP_<AIRPORT>_<AIRLINE>_<MONTH>_<YEAR>
         * @param values    The list of FlightDataWrtiables containing the normalized delays & the nuber of flights
         * @param context   The context of this jobs configuration
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<FlightDataWritable> values, Context context)
                throws IOException, InterruptedException {

            int totalFlights = 0;
            double totalDelays = 0;
            for (FlightDataWritable val : values){
                totalFlights += val.getFlights().get();
                totalDelays += val.getNormalizedDelay().get();
            }
            double meanDelay = totalDelays / totalFlights;
            String keys[] = key.toString().split("_");      //Split key by _
            String resultKey = keys[1]+","+keys[2]+","+keys[3]+","+keys[4];
            context.write(new Text(resultKey), new DoubleWritable(meanDelay));
        }
    }

    /**
     * The partitioner class paritions each airport or airline by the month. For example, for all airlines the data for
     * January goes into reducer 0 and so on till 11 for December. Similarly, for all airports the data for January goes
     * to reducer 12 and so on till 23 for December. The partitioner finds out whether the current record is for airport
     * or airline by the first 2 characters in the key which are made either "AP" or "AL" and find out the month as the
     * 4th element in the key when split on _ since key is either AL_<AIRLINE>_<AIRPORT>_<MONTH>_<YEAR> or
     * AP_<AIRPORT>_<AIRLINE>_<MONTH>_<YEAR>
     */
    public static class AverageMonthlyPartitioner extends Partitioner<Text,FlightDataWritable> {

        @Override
        public int getPartition(Text text, FlightDataWritable flightDataWritable, int i) {
            String keys[] = text.toString().split("_");
            String airlineOrAirport = keys[0];
            String month = keys[3];
            if(airlineOrAirport.equals("AL"))
                return Integer.parseInt(month) - 1;
            else
                return Integer.parseInt(month) + 11;
        }
    }
}
