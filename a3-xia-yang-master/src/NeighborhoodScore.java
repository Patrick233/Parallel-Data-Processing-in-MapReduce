
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by Yang Xia on 2017/9/27.
 */
public class NeighborhoodScore {

  final static int K = 2;

  // copyright from stackoverflow
  // I modified it a bit so as to fit in the case
  public static Configuration sumLetters(String outPath) throws IOException {
    Configuration configuration = new Configuration();
    configuration.set("K", String.valueOf(K));
    FileSystem fs = FileSystem.get(configuration);
    long total = 0;

    for (FileStatus fileStat : fs.globStatus(new Path(outPath + "/part-r-*"))) {
      BufferedReader reader = new BufferedReader(
          new InputStreamReader(fs.open(fileStat.getPath())));
      String nextLine;
      while ((nextLine = reader.readLine()) != null) {
        String tokens[] = nextLine.split("\t");
        total += Integer.parseInt(tokens[1]);
      }
      reader.close();
    }

    for (FileStatus fileStat : fs.globStatus(new Path(outPath + "/part-r-*"))) {
      BufferedReader reader = new BufferedReader(
          new InputStreamReader(fs.open(fileStat.getPath())));
      String nextLine;
      while ((nextLine = reader.readLine()) != null) {
        String tokens[] = nextLine.split("\t");
        double letterCount = Double.parseDouble(tokens[1]);
        double frequency = letterCount / total;

        if (frequency >= 0.1) {
          configuration.set(tokens[0], "0");
        } else if (frequency >= 0.08) {
          configuration.set(tokens[0], "1");
        } else if (frequency >= 0.06) {
          configuration.set(tokens[0], "2");
        } else if (frequency >= 0.04) {
          configuration.set(tokens[0], "4");
        } else if (frequency >= 0.02) {
          configuration.set(tokens[0], "8");
        } else if (frequency >= 0.01) {
          configuration.set(tokens[0], "16");
        } else {
          configuration.set(tokens[0], "32");
        }
      }
      reader.close();

    }

    return configuration;
  }


  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: NeighbouhoodScore <input path> <output1 path> <output2 path>");
      System.exit(-1);
    }

    Job job = new Job();
    job.setJobName("CountLetter");
    job.setJarByClass(NeighborhoodScore.class);

    //FileInputFormat.setInputDirRecursive(job, true);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(CountLetterMapper.class);
    job.setReducerClass(CountLetterReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    if (job.waitForCompletion(true)) {
      Configuration conf = sumLetters(args[1]);

      Job calculation = new Job(conf);
      calculation.setJobName("NeighbourScore");
      calculation.setJarByClass(NeighborhoodScore.class);

      FileInputFormat.addInputPath(calculation, new Path(args[0]));
      FileOutputFormat.setOutputPath(calculation, new Path(args[2]));

      calculation.setMapOutputKeyClass(Text.class);
      calculation.setMapOutputValueClass(IntWritable.class);

      calculation.setMapperClass(CalculateScoreMapper.class);
      calculation.setReducerClass(CalculateScoreReducer.class);

      calculation.setOutputKeyClass(Text.class);
      calculation.setOutputValueClass(DoubleWritable.class);
      calculation.waitForCompletion(true);
    } else {
      System.exit(1);
    }

  }
}
