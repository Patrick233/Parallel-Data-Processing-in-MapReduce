import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.*;
import java.io.IOException;

/**
 * @author Ankita, Ushang, Xia
 */
public class Driver {

    /**
     * This function runs the seond MR job of performing the joins and predicting the flights
     * @param conf  The configuration object for this job
     * @param input The input string needed for the job
     * @param src   The src airport of the prediction
     * @param dest  The dest airport of the pedicition
     * @param args  The arguments passed for input and output
     */
    public static void predictionJob(Configuration conf, String input,String src, String dest, String[] args)throws IOException, InterruptedException, ClassNotFoundException{
        Job j = Job.getInstance(conf,"Prediction Job");
        j.setJarByClass(FlightPrediction.class);
        j.setMapperClass(FlightPrediction.FlightPredictionMapper.class);
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(LongWritable.class);
        j.setReducerClass(FlightPrediction.FlightPredictionMapper.FlightPredictionReducer.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(LongWritable.class);
        j.setInputFormatClass(NonSplitableTextInputFormat.class);
        j.getConfiguration().set("input",input);
        FileInputFormat.addInputPath(j, new Path(args[1]+"/phase1output"));
        FileOutputFormat.setOutputPath(j, new Path(args[1]+"/finalOutput/"+src+"-"+dest));
        j.waitForCompletion(true);
    }

    /**
     * This function performs the first MR job of finding all single hop routes for the year of prediction ie for all
     * years before the prediction is to be made.
     * @param conf  The configuraion object for this job
     * @param year  The year for which the routes are found
     * @param args  The arguments passed for input and output
     */
    public static void singleHopJob(Configuration conf, String year,String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        Job j = Job.getInstance(conf,"Single Hops");
        j.setJarByClass(SingleHopFlights.class);
        j.setMapperClass(SingleHopFlights.SingleHopFlightsMapper.class);
        j.setPartitionerClass(SingleHopFlights.FlightsPartitioner.class);
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(DelayWritable.class);
        j.setReducerClass(SingleHopFlights.SingleHopFlightsReducer.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);
        j.setInputFormatClass(NonSplitableTextInputFormat.class);
        j.getConfiguration().set("year",year);
        j.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
        j.setNumReduceTasks(12);
        FileInputFormat.addInputPath(j, new Path(args[0]));
        FileOutputFormat.setOutputPath(j, new Path(args[1]+"/phase1output"));
        j.waitForCompletion(true);
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        Configuration conf = new Configuration();
        InputStream in = Driver.class.getResourceAsStream("inputFile");
        try(BufferedReader br = new BufferedReader(new InputStreamReader(in)))      //read input line by line
        {
            String line = br.readLine();
            String[] arr = line.split(",");
            String yr = arr[0];
            singleHopJob(conf,yr,args);
            System.out.println("--------- Starting Predictions ---------");
            while (line != null) {
                String[] array = line.split(",");
                String src = array[3];
                String dest = array[4];
                String input = array[0]+","+array[1]+","+array[2]+","+array[3]+","+array[4];
                predictionJob(conf,input,src,dest,args);
                line = br.readLine();
            }
        }
    }
}
