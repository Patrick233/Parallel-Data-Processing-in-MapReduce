
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * The Driver class runs both our Map Reduce jobs
 * @author Ankita,Ushang,Xia
 */
public class Driver {

    /**
     * Configures & Performs the first Map Reduce job to find out the top 5 airports and airlines over the entire corpus
     * @param conf The configuration object needed to run the job
     * @param args  The arguments passed on the command line for input & output directory
     * @throws IOException: If the input files are not found while reading
     *         InterruptedException: If the job gets terminated in the interim
     *         ClassNotFoundException: If the mapper/reducer/combiner are not found at runtime
     */
    public static void top5Job(Configuration conf, String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        Job j = Job.getInstance(conf,"Top 5");
        j.setJarByClass(ActiveAirportAirlines.class);
        j.setMapperClass(ActiveAirportAirlines.ActiveAirportAirlinesMapper.class);
        j.setPartitionerClass(ActiveAirportAirlines.ActivePartitioner.class);
        j.setCombinerClass(ActiveAirportAirlines.ActiveAirportAirlinesReducer.class);
        j.setReducerClass(ActiveAirportAirlines.ActiveAirportAirlinesReducer.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);
        j.setInputFormatClass(NonSplitableTextInputFormat.class);
        j.setNumReduceTasks(2);
        FileInputFormat.addInputPath(j, new Path(args[0]));
        FileOutputFormat.setOutputPath(j, new Path(args[1] + "/phase1output"));
        j.waitForCompletion(true);
    }

    /**
     * Configures & Performs the second Map Reduce job to find out the average monthly delay
     * @param conf The configuration object needed to run the job
     * @param args  The arguments passed on the command line for input & output directory
     * @throws IOException: If the input files are not found while reading
     *         InterruptedException: If the job gets terminated in the interim
     *         ClassNotFoundException: If the mapper/reducer/combiner are not found at runtime
     */
    public static void averageMonthlyDelayJob(Configuration conf, String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        Job j2 = Job.getInstance(conf,"Average Monthly Delay");
        j2.setJarByClass(AverageMonthlyDelay.class);
        j2.setMapperClass(AverageMonthlyDelay.AverageMonthlyDelayMapper.class);
        j2.setPartitionerClass(AverageMonthlyDelay.AverageMonthlyPartitioner.class);
        j2.setReducerClass(AverageMonthlyDelay.AverageMonthlyDelayReducer.class);
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(FlightDataWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(DoubleWritable.class);
        j2.setInputFormatClass(NonSplitableTextInputFormat.class);
        j2.getConfiguration().set("phase1output",args[1] + "/phase1output");
        j2.setNumReduceTasks(24);
        j2.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
        FileInputFormat.addInputPath(j2, new Path(args[0]));
        FileOutputFormat.setOutputPath(j2, new Path(args[1] + "/phase2output"));
        j2.waitForCompletion(true);

    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        Configuration conf = new Configuration();
        top5Job(conf,args);
        averageMonthlyDelayJob(conf,args);
    }
}
