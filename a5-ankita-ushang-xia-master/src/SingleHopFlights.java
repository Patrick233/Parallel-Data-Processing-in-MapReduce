import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Ankita Patel
 */
public class SingleHopFlights {

    public static class SingleHopFlightsMapper extends Mapper<Object, Text, Text, DelayWritable> {

        private static String year = null;
        private static HashMap<String, DelayWritable> flights;
        private CSVParser csvParser = new CSVParser(',', '"');

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            year = context.getConfiguration().get("year");
            flights = new HashMap<>();
        }

        /**
         * The map function reads the CSV line by line and finds all the single hop flights for all the years prior to
         * the prediction year
         * @param key       The long offset which we can neglect
         * @param value     The line read from CSV
         * @param context   The context of this jobs configuration
         */
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] inVal = csvParser.parseLine(value.toString());
            if(UtilityHelper.isRecordValidAndRequired(inVal,year)) {    //Record is clean and needed
                double delay = 0;
                if(Double.parseDouble(inVal[42]) > 0)
                    delay = Double.parseDouble(inVal[42]);
                if(Integer.parseInt(inVal[47]) == 1)       //Flight is cancelled
                    delay = 12*60; //Making the delay of the canceled flight to 12hrs since it guarantees that next flight is missed

                String outKey = inVal[14]+"_"+inVal[23]+"_"+inVal[29]+"_"+inVal[40]+"_"+inVal[2]+"_"+inVal[8];
                //Add the data to flights map
                UtilityHelper.addToHashmap(flights,delay,outKey);
            }
        }

        /**
         * The cleanup method writes the key value pair out to the reducer
         * @param context The context of this methods configuration
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(Map.Entry<String, DelayWritable> pair: flights.entrySet())
                context.write(new Text(pair.getKey()),pair.getValue());
        }
    }

    public static class FlightsPartitioner extends Partitioner<Text, DelayWritable> {

        /**
         * The partition function partitions the output of the mapper to 12 reducers one for each month
         * @param text              The input string received from mapper. It is of the form
         *                          <SRC>_<DEST>_<ARR-TIME>_<DEPT-TIME>_<MONTH>_<AIRLINE>
         * @param delayWritable     The value of the map output consisting delay information
         * @return                  The partition based on month from the key
         */
        @Override
        public int getPartition(Text text, DelayWritable delayWritable, int i) {
            String keys[] = text.toString().split("_");
            return Integer.parseInt(keys[4])-1;
        }
    }

    public static class SingleHopFlightsReducer extends Reducer<Text, DelayWritable, Text, DoubleWritable> {
        /**
         * The reducer aggregates all the normalized delays and the total flights sent by the mapper for each key and
         * finds the mean normalized delay for the same
         * @param key       The key sent by the mapper. It will be in the form of
         *                  <SRC>_<DEST>_<DEPT_TIME>_<ARR_TIME>_<MONTH>_<AIRLINE>
         * @param values    The list of DelayWrtiables containing the agregted delays & the nuber of flights
         * @param context   The context of this jobs configuration
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<DelayWritable> values, Context context)
                throws IOException, InterruptedException {

            int totalFlights = 0;
            double totalDelays = 0;
            for (DelayWritable val : values){
                totalFlights += val.getFlights().get();
                totalDelays += val.getDelay().get();
            }

            double meanDelay = totalDelays / totalFlights;
            String keys[] = key.toString().split("_");      //Split key by _
            String resultKey = keys[0] + "," + keys[1] + "," + keys[2] + "," + keys[3] + "," + keys[4] + "," + keys[5];
            context.write(new Text(resultKey), new DoubleWritable(meanDelay));

        }
    }
}

