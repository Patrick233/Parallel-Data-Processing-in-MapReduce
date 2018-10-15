import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by Patrizio on 2017/9/28.
 */
public class CalculateScoreMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    Configuration configuration = context.getConfiguration();
    int K = Integer.parseInt(configuration.get("K"));
    String line = value.toString();
    if(!line.trim().isEmpty()) {
      line = line.replaceAll("[^a-zA-Z ]", "");
      line = line.toLowerCase();
      String[] subCorpus = line.split(" ");
      for (int i = K; i < subCorpus.length - K; i++) {
        List<String> kNeighbor = new ArrayList<>();
        int count = 0;
        while (count<K){
          kNeighbor.add(subCorpus[i-K+count]);
          count++;
        }
        while (count>0){
          kNeighbor.add(subCorpus[i+count]);
          count--;
        }
        int score = kNeighborhoodScore(kNeighbor, configuration);
        context.write(new Text(subCorpus[i]), new IntWritable(score));
      }
    }
  }


  public static int kNeighborhoodScore(List<String> kNeighbor, Configuration configuration){
    int res =0;
    for(int i=0;i<kNeighbor.size();i++){
      String word = kNeighbor.get(i);
      if(word!=null) {
        for (int j = 0; j < word.length(); j++) {
          char c = word.charAt(j);
          if (Character.isLetter(c)) {
            int charScore = Integer.parseInt(configuration.get(String.valueOf(c)));
            res += charScore;
          }
        }
      }
    }
    return res;
  }

}
