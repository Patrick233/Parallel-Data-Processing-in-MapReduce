
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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
    List<Integer> scoreList = new ArrayList<>();

    Comparator<Integer> scoreComparator = new Comparator<Integer>() {
      @Override
      public int compare(Integer score1, Integer score2) {
        return score1 - score2;
      }
    };

    for (IntWritable value : values) {
      int score = value.get();
      scoreList.add(score);
    }
    scoreList.sort(scoreComparator);
    int listSize = scoreList.size() / 2;
    int median = 0;
    if (listSize > 0) {
      if (listSize % 2 != 0) {
        median = scoreList.get(listSize / 2);
      } else {
        median = scoreList.get(listSize / 2 - 1) + scoreList.get(listSize / 2);
      }
    }
    context.write(key, new DoubleWritable(median));
  }
}
