import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Patrizio on 2017/10/19.
 */
public class FligthValidation {

    public static class FligthValidationMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private static List<String> flightList = new ArrayList<String>();
        private static String year, month, date;
        private static HashMap<String, Double> flightsDelay= new HashMap<>();
        private CSVParser csvParser = new CSVParser(',', '"');

        /**
         * This function reads the flightlist from the context
         * @param context The context of this jobs configuration
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            year = context.getConfiguration().get("year");
            month = context.getConfiguration().get("month");
            date = context.getConfiguration().get("date");

            //String flights = readFile("output/finalOutput/DEN-DCA/",context);
            String flights = context.getConfiguration().get("flightList");
            String[] twoHops = flights.split("/");
            for (String f : twoHops) {
                String[] singleHop = f.split("@");
                flightList.add(singleHop[0]);
                flightList.add(singleHop[1]);
            }
        }

        /**
         * The map function finds the dely for the flights
         * @param key       The long offset we can neglect
         * @param value     The files read line by line
         * @param context   The context of this jobs configuration
         */
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] inVal = csvParser.parseLine(value.toString());
            if (isRecordValidAndRequired(inVal, flightList)) {    //Record is clean and needed
                double delay = 0;
                if (Double.parseDouble(inVal[42]) > 0)
                    delay = Double.parseDouble(inVal[42]);
                if (Integer.parseInt(inVal[47]) == 1)       //Flight is cancelled
                    delay = 12 * 60;

                String outKey = year+","+month+","+date+","+inVal[29]+","+ "," + inVal[40]+ "," + inVal[8]+","+
                        inVal[14] + "," + inVal[23];
                flightsDelay.put(outKey, delay);
            }
        }

        /** This function checks the correctness of the prediction to the actual input for both hops
         * @param context       The context of this jobs configuration
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (int i = 0; i < flightList.size()-1; i +=2) {
                // Check if first flight meets layover restrictions
                String[] record1 = flightList.get(i).split(",");
                long firstFlightArrivalTime = UtilityHelper.getTimeInSeconds(record1, 4);
                String[] record2 = flightList.get(i + 1).split(",");
                long secFlightDeptTime = UtilityHelper.getTimeInSeconds(record2, 3);

                double delay1 = flightsDelay.getOrDefault(flightList.get(i),120.0);

                String outKey = flightList.get(i) +"@"+ flightList.get(i + 1);
                if (!checkWithinTimeLimits(firstFlightArrivalTime, secFlightDeptTime, delay1)) {
                    context.write(new Text(outKey), new DoubleWritable(-100.0));
                    continue;
                }

                double delay2 = flightsDelay.getOrDefault(flightList.get(i+1),120.0);

                if (delay2 > 0) {
                    context.write(new Text(outKey), new DoubleWritable(-100.0));
                    continue;
                }

                context.write(new Text(outKey), new DoubleWritable(1.0));
            }

        }


        /**
         * A helper method to check correctness of record
         * @param inval         The record to be checked
         * @param flightList    The list of flights to be validated
         * @return              True if the record is required false otherwise
         */
        public static boolean isRecordValidAndRequired(String[] inval, List<String> flightList) {
            String outKey = year+","+month+","+date+","+inval[29]+","+ "," + inval[40]+ "," + inval[8]+","+
                    inval[14] + "," + inval[23];
            if (inval == null || inval.length == 0) //If row is empty don't do anything
            {
                return false;
            }

            try {
                if (!(inval[0].equals(year) && inval[2].equals(month) && inval[3].equals(date))) return false;

                for (String flight : flightList) {
                    if (!flight.equals(outKey)) {
                        return false;
                    }
                }
            } catch (Exception e) {
                return false;
            }

            if (UtilityHelper.checkIfNonZero(inval) && UtilityHelper.checkIfNotEmpty(inval)
                    && UtilityHelper
                    .timezoneCheck(inval)) //Now check its validity
            {
                return true;
            }
            return false;
        }

        /**
         * Helper method to check if connection is in limits
         * @param arr   The arrival time of first flight
         * @param dept  The departure time of second flight
         * @param delay The delay of first flight
         * @return      True if connection can be made false otherwise
         */
        public static boolean checkWithinTimeLimits(Long arr, Long dept, Double delay) {
            final int SECONDS_45_MIN = 45 * 60;
            final int SECONDS_12_HRS = 12 * 60 * 60;
            double layoverTime = dept - arr - delay;
            if (layoverTime > SECONDS_45_MIN && layoverTime < SECONDS_12_HRS) {
                return true;
            }
            return false;
        }

    }
    public static class FlightValidationReducer extends Reducer<Text, LongWritable, Text, DoubleWritable> {

        /**
         * The reducer just writes files to the output. Its an identity reducer
         */
        public void reduce(Text key, DoubleWritable values, Context context)
                throws IOException, InterruptedException {

            context.write(key, values);
        }
    }
}


