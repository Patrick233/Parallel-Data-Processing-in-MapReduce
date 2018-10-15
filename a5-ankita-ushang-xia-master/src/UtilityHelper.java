import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Ankita, Ushang, Xia
 */
public class UtilityHelper {

    /**
     * Checks if the record is required(non empty) and valid(non zero and number and within limits)
     * @param record The record that needs to be checked
     * @return boolean True if the record is valid & required else false
     */
    public static boolean isRecordValidAndRequired(String[] record, String year){

        if (record == null || record.length == 0) //If row is empty don't do anything
            return false;

        try{
            if(Integer.parseInt(record[0])>=Integer.parseInt(year) && Integer.parseInt(record[0]) < 1989)  //If year is equal or greater then the prediction year return false
                return false;
        }
        catch(Exception e){
            return false;
        }

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
                record[2].isEmpty() || record[42].isEmpty())
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
                    Integer.parseInt(record[3])>0 && Integer.parseInt(record[3])<32 &&   // Check for day correctness
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
     * This function adds to the hashmap in the first MR job by taking the key and a Delay Writable hashmap
     * @param target    The target hashmap for the value to be inserted to
     * @param delay     The delay of the flight
     * @param key       Each uniquely identified flight from source to dest at a given time for a given airline
     */
    public static void addToHashmap(HashMap<String, DelayWritable> target, double delay, String key){
        DelayWritable temp;
        if(target.containsKey(key)){
            temp = target.get(key);
            temp.setFlights(new IntWritable(temp.getFlights().get()+1));
            temp.setDelay(new DoubleWritable(temp.getDelay().get()+delay));
        }
        else{
            temp =  new DelayWritable();
            temp.setFlights(new IntWritable(1));
            temp.setDelay(new DoubleWritable(delay));
        }
        target.put(key,temp);
    }

    /**
     * This function is used to create the two hashmaps for the join in the second MR task
     * @param record        The record which is being inserted
     * @param target        The target hashmap in which the record needs to be inserted
     * @param keyToCheck    The key which tells us whether its the source or destination hashmap
     */
    public static void addToHashMap(String[] record, HashMap<String,HashMap<Long, FlightDataWritable>> target,
                                    int keyToCheck){
        HashMap<Long, FlightDataWritable> temp;

        long deptTime = getTimeInSeconds(record,2);
        long arrTime = getTimeInSeconds(record,3);

        FlightDataWritable ft = new FlightDataWritable();
        ft.setCarrier(new Text(record[5]));
        ft.setDest(new Text(record[1]));
        ft.setSrc(new Text(record[0]));
        ft.setArrivalTime(new LongWritable(Long.parseLong(record[3])));
        ft.setDeptTime(new LongWritable(Long.parseLong(record[2])));
        ft.setDelay(new DoubleWritable(Double.parseDouble(record[6])));
        ft.setMonth(new IntWritable(Integer.parseInt(record[4])));

        if(keyToCheck == 0){    //Flight leaves from source : store destination data
            if(target.containsKey(record[1])){
                temp = target.get(record[1]);
                temp.put(arrTime,ft);
            }
            else{
                temp = new HashMap<>();
                temp.put(arrTime,ft);
            }
            target.put(record[1],temp);
        }
        else{                   // Flight arrives at dest : store source data
            if(target.containsKey(record[0])){
                temp = target.get(record[0]);
                temp.put(deptTime,ft);
            }
            else{
                temp = new HashMap<>();
                temp.put(deptTime,ft);
            }
            target.put(record[0],temp);
        }
    }

    /**
     * This function finds if the two connections will be possible by checking the condition that there are atleast 45 mins
     * between two flights(even after delay of the first) and no more than 12 hours.
     * @param src       The hashmap that stores the flights to destination of the first hop
     * @param dest      The hashmap that stores the flights to origin of the second hop
     * @param input     The prediction input
     * @param target    The hashmap which stores the result of the join and the score of those flights
     */
    public static void getSchedule(HashMap<Long, FlightDataWritable> src, HashMap<Long, FlightDataWritable> dest, String[] input,
                                   HashMap<String,Long> target){
        for(Map.Entry<Long, FlightDataWritable> srcPair: src.entrySet()){
            Long srcKey = srcPair.getKey();
            FlightDataWritable srcVal = srcPair.getValue();
            for(Map.Entry<Long, FlightDataWritable> destPair: dest.entrySet()){
                Long destKey = destPair.getKey();
                FlightDataWritable destVal = destPair.getValue();
                if(checkWithinTimeLimits(srcKey,destKey,srcVal.getDelay().get())){
                    scoreSchedule(srcVal,destVal,input,target);
                }
            }
        }
    }

    /**
     * This function assigns a score to the schedule and adds it to the final hashmap
     * @param srcVal    The information of the first flight
     * @param destVal   The information of the second flight
     * @param input     The prediction inpit
     * @param target    The hashmap which stores the result of the join and the score of those flights
     */
    private static void scoreSchedule(FlightDataWritable srcVal, FlightDataWritable destVal,String[] input,
                                      HashMap<String,Long> target) {
        long score = 0;
        String key = "[("+input[0]+","+input[1]+","+input[2]+","+srcVal.getDeptTime()+","+srcVal.getArrivalTime()+","+srcVal.getCarrier()+
                ","+srcVal.getSrc()+","+srcVal.getDest()+"),"+
                "("+input[0]+","+input[1]+","+input[2]+","+destVal.getDeptTime()+","+destVal.getArrivalTime()+","+destVal.getCarrier()+
                ","+destVal.getSrc()+","+destVal.getDest()+")]";

        if(destVal.getDelay().get() > 1)
            score+= -100;
        else
            score+= 1;

        Long currScore = target.getOrDefault(key, 0L);
        target.put(key,score+currScore);
    }

    /**
     * This function verifies the condition that Connections must have at least 45 minutes between landing and takeoff,
     * and no more than 12 hours
     * @param arr   The arrival time of the first flight in seconds from midnight
     * @param dept  The departure time of the second flight in seconds from midnight
     * @param delay The delay of the first flight
     * @return True if the flight is within limits, false otherwise
     */
    public static boolean checkWithinTimeLimits(Long arr, Long dept, Double delay){
        final int SECONDS_45_MIN = 45 * 60;
        final int SECONDS_12_HRS = 12 * 60 * 60;
        double layoverTime = dept - arr - delay;
        if(layoverTime > SECONDS_45_MIN && layoverTime < SECONDS_12_HRS)
            return true;
        return false;
    }

    /**
     * This function converts the time to seconds from midnight
     * @param record    The record for which the time in seconds in desired
     * @param key       The key of this record
     * @return          The time in seconds from midnight
     */
    public static long getTimeInSeconds(String[] record, int key){
        String time = record[key];
        int mins = Integer.parseInt(time.substring(time.length()-2));
        int hrs  = Integer.parseInt(time.substring(0,time.length()-2));
        long seconds = (mins * 60) + (hrs * 60 * 60);
        return seconds;
    }

    /**
     * Checks if the record is required(only for the month we want to predict) and valid(not a direct flight and one
     * which matches either our prediction src or dest)
     * @param record The record that needs to be checked
     * @return boolean True if the record is valid & required else false
     */
    public static boolean isRouteRequired(String[] record, String[] input){

        if(record[4].equals(input[1]) == false)      //Only check flights for that month
            return false;

        if(record[0].equals(input[3]) && record[1].equals(input[4]))   //If its a direct flight
            return false;

        if(record[0].equals(input[3]) || record[1].equals(input[4]))    //Its either matching our src or dest
            return true;

        return false;
    }

}

/**
 * @author Tom White
 * This class is used to force that each file goes exactly to one Mapper
 */
class NonSplitableTextInputFormat extends TextInputFormat {

    @Override
    protected boolean isSplitable(JobContext context, Path path) {
        return false;
    }
}