import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Ankita,Ushang,Xia
 */
public class ActiveAirportAirlines {

    public static class ActiveAirportAirlinesMapper extends Mapper<Object, Text, Text, IntWritable> {

        // initialize CSVParser to read input as comma separated values
        private CSVParser csvParser = new CSVParser(',', '"');
        HashMap<String,Integer> airlineCount = new HashMap<>();
        HashMap<String,Integer> airportCount = new HashMap<>();

        /**
         * The map function checks if the airline and the airport fields are present in the given CSV file line and then
         * increments the corresponding counter by 1
         * @param key           Long offset which we can ignore
         * @param value         Each line of the CSV input file
         * @param context       The context of this jobs configuration
         * @throws IOException  If the file cannot be read/found
         * @throws InterruptedException
         */
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] inVal = csvParser.parseLine(value.toString());
            if (inVal == null || inVal.length == 0)         // Check if record is null or empty
                return;

            if (inVal[8] != null)      //check if airline value is present
                airlineCount.put(inVal[8], airlineCount.getOrDefault(inVal[8], 0) + 1);
            if (inVal[23] != null)       //check if airport value is present
                airportCount.put(inVal[23], airportCount.getOrDefault(inVal[23], 0) + 1);
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
            Iterator it = airlineCount.entrySet().iterator();
            Iterator it2 = airportCount.entrySet().iterator();
            while(it.hasNext()){
                Map.Entry pair = (Map.Entry)it.next();
                context.write(new Text(String.valueOf("AL_"+pair.getKey())),new IntWritable((int)pair.getValue()));
            }
            while(it2.hasNext()){
                Map.Entry pair = (Map.Entry)it2.next();
                context.write(new Text(String.valueOf("AP_"+pair.getKey())),new IntWritable((int)pair.getValue()));
            }
        }
    }

    public static class ActiveAirportAirlinesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        /**
         * The reduce function adds up the total count of the airport and the lines sent by the mapper
         * @param key       It is the key sent by mapper in format AL_<AIRLINE-NAME> or AP_<AIRPORT-NAME>
         * @param values    The list of values of counts for each key
         * @param context   The context of this jobs execution
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();
            result.set(sum);

            context.write(key, result);
        }
    }

    /**
     * This class partitions the key from the mapper into 2 different reducers by using the AL or the AP prepended in
     * the key
     */
    public static class ActivePartitioner extends Partitioner<Text,IntWritable>{

        @Override
        public int getPartition(Text text, IntWritable intWritable, int i) {
            if(text.toString().substring(0,2).equals("AL")) return 0;
            return 1;
        }
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
