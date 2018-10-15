import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Xia on 2017/10/19.
 */
public class ValidationDriver {

    // Conf.get("flightList") should give me a list of flights seperated by /
    public static void ValidationJob(Configuration conf, String input) throws IOException, InterruptedException, ClassNotFoundException {
        Job j = Job.getInstance(conf, "Validation Job");
        j.setJarByClass(FligthValidation.class);
        j.setMapperClass(FligthValidation.FligthValidationMapper.class);
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(DoubleWritable.class);

        j.setNumReduceTasks(1);
        j.setReducerClass(FligthValidation.FlightValidationReducer.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        j.setInputFormatClass(NonSplitableTextInputFormat.class);

        FileInputFormat.addInputPath(j, new Path(input));
        FileOutputFormat.setOutputPath(j, new Path("output/ValidationOutput"));
        j.waitForCompletion(true);

    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("year", "2001");
        conf.set("month", "3");
        conf.set("date", "1");
        String flights = "2001,2,1,1015,1329,US,BOS,MCO@2001,2,1,1849,2137,UA,MCO,LAX" +"/"+
                "2001,2,1,1015,1331,B6,BOS,MCO@2001,2,1,1849,2137,UA,MCO,LAX"+"/"+
                "2001,2,1,1100,1427,DL,BOS,MCO@2001,2,1,1750,2028,UA,MCO,LAX"+"/"+
                "2001,2,1,1140,1457,DL,BOS,MCO@2001,2,1,1745,2028,UA,MCO,LAX"+"/"+
                "2001,2,1,1200,1502,DL,BOS,ATL@2001,2,1,1758,2004,DL,ATL,LAX";
        conf.set("flightList", flights);

        ValidationJob(conf, args[0]);
    }


}
