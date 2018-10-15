
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by Patrizio on 2017/9/28.
 */
public class CalculateScoreReducer
    extends Reducer<Text, IntWritable, Text, DoubleWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    int totalOccurance = 0;
    int totalScore = 0;
    for (IntWritable value : values) {
      totalOccurance += 1;
      totalScore +=value.get();
    }
    double meanScore = 1.0*totalScore/totalOccurance;
    context.write(key, new DoubleWritable(meanScore));
  }
}
