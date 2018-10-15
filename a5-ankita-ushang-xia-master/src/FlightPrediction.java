import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Ushang Thakker
 */
public class FlightPrediction {

    public static class FlightPredictionMapper extends Mapper<Object, Text, Text, LongWritable> {

        private static HashMap<String,HashMap<Long, FlightDataWritable>> src = new HashMap<>();
        private static HashMap<String,HashMap<Long, FlightDataWritable>> dest = new HashMap<>();
        private static HashMap<String,Long> scoredFlights = new HashMap<>();

        private CSVParser csvParser = new CSVParser(',', '"');
        private static String[] input;

        /**
         * This function get the prediction input string from the context at the start of every mapper
         * @param context   The context of this jobs configuration
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            input = csvParser.parseLine(context.getConfiguration().get("input"));
        }

        /**
         * The map function readies the hashmpas for the joins for the flights of the same prediciton month & of the same
         * source to the destination as the one to be predicted.
         * @param key       The long offset of each line which we can ignore
         * @param value     Each line of files created by output of phase 1
         * @param context   The context object for this job
         */
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] inVal = csvParser.parseLine(value.toString());

            if(UtilityHelper.isRouteRequired(inVal,input))
                if(inVal[0].equals(input[3]))                           //add to src hashmap
                    UtilityHelper.addToHashMap(inVal,src,0);
                else if(inVal[1].equals(input[4]))                     //add to dest hashmap
                    UtilityHelper.addToHashMap(inVal,dest,1);
                else
                    return;
        }

        /**
         * The cleanup function perfrorms the actual join once the map functions have created the hashmaps for it and does
         * the scoring based on flight delays. Finally it also writes the output to the reducer
         * @param context The context of this jobs configuration
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(Map.Entry<String,HashMap<Long, FlightDataWritable>> srcPair: src.entrySet()){
                String srcKey = srcPair.getKey();
                HashMap<Long, FlightDataWritable> srcValue = srcPair.getValue();
                HashMap<Long, FlightDataWritable> destValue;
                if(dest.containsKey(srcKey)){
                    destValue = dest.get(srcKey);
                    UtilityHelper.getSchedule(srcValue,destValue,input,scoredFlights);
                }
            }

            for(Map.Entry<String,Long> scorePair: scoredFlights.entrySet()){
                context.write(new Text(scorePair.getKey()),new LongWritable(scorePair.getValue()));
            }
        }


        public static class FlightPredictionReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

            /**
             * The reduce function aggregates the total score for the similar 2 hop flights reaching the desired
             * destination.
             * @param key       The entire itinerary for source to destionation
             * @param values    The list of scores received by each flight
             * @param context   The context of this jobs configuration
             */
            public void reduce(Text key, Iterable<LongWritable> values, Context context)
                    throws IOException, InterruptedException {

                Long totalScore = 0L;
                for (LongWritable val : values){
                    totalScore += val.get();
                }

                if(totalScore>0)
                    context.write(key, new LongWritable(totalScore));
            }
        }



    }
}
