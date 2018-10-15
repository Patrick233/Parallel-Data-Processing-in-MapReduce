import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * Created by Patrizio on 2017/9/28.
 */
public class CountLetterMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    Map<Character,Integer> frequencyMap = new HashMap<>();
    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      if (Character.isLetter(c)) {
        frequencyMap.put(c,frequencyMap.getOrDefault(c,1)+1);
      }
    }
    for(char letter:frequencyMap.keySet()){
      IntWritable frequency = new IntWritable(frequencyMap.get(letter));
      context.write(new Text(String.valueOf(letter)),frequency);
    }
  }

}
