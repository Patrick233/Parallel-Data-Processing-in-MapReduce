import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by Patrizio on 2017/9/14.
 */
public class NeighborhoodScore {

  //private Map<String,Integer> map = new HashMap<>();
  private String[] corpus;

  public NeighborhoodScore(String[] corpus){
    this.corpus =corpus;
  }


  // Given the frequency array and total number of letters, calculate score for each letter
  // Return the score map in an array
  public static int[] getScore(int[] letterCount, int totalLetters) {
    int len = letterCount.length;
    int[] letterSocre = new int[len];
    for (int i = 0; i < letterCount.length; i++) {
      double frequency = 1.0 * letterCount[i] / totalLetters;
      int score;
      if (frequency >= 0.1) {
        score = 0;
      } else if (frequency >= 0.08 && frequency < 0.1) {
        score = 1;
      } else if (frequency >= 0.06 && frequency < 0.8) {
        score = 2;
      } else if (frequency >= 0.04 && frequency < 0.6) {
        score = 4;
      } else if (frequency >= 0.02 && frequency < 0.4) {
        score = 8;
      } else if (frequency >= 0.01 && frequency < 0.2) {
        score = 16;
      } else {
        score = 32;
      }
      letterSocre[i] = score;
    }
    return letterSocre;
  }

  public int wordScore(String s, int[] letterScore){
    int res=0;
    for(char c:s.toCharArray()){
      if(c-'a'>=0 && c-'z'<=0){
        res += letterScore[c- 'a'];
      }
    }
    return  res;
  }

  public List<String> kNeighborhood(int k, int pos){
    List<String> res = new ArrayList<>();
    int start,end;
    if(pos-k<0 && pos+k>=corpus.length){
        start = 0;
        end = corpus.length;
    }
    else if(pos-k<0){
        start = 0;
        end = pos+k+1;
    }
    else if(pos+k>=corpus.length){
      start = pos-k;
      end = corpus.length;
    }
    else {
      start = pos-k;
      end = pos+k+1;
    }
    for(int i=start;i<end;i++){
      if(i!=pos){
        res.add(corpus[i]);
      }
    }

    return res;
  }

  public int kNeighborhoodScore(int k, int pos, int[] letterscore){
    List<String> kNeighbor = kNeighborhood(k,pos);
    int res =0;
    for(int i=0;i<kNeighbor.size();i++){
      res += wordScore(kNeighbor.get(i),letterscore);
    }
    return res;
  }

  public Map<String,Double> scoreMap(int k, int[] letterScore){
    Map<String,Double> res = new HashMap<>();
    Map<String,Integer> count = new HashMap<>();
    Map<String,Integer> sumScore = new HashMap<>();

    for(int i=0;i<corpus.length;i++){
      String s = corpus[i];
      int score = kNeighborhoodScore(k,i,letterScore);
      if(sumScore.containsKey(s)){
        sumScore.put(s,sumScore.get(s)+score);
        count.put(s,count.get(s)+1);
      }
      else {
        sumScore.put(s,score);
        count.put(s,1);
      }
    }

    for (String key:sumScore.keySet()){
      double mean = 1.0*sumScore.get(key)/count.get(key);
      res.put(key,mean);
    }
    return res;
  }

  public void toCSV(int k, int[] letterScore, String name){
    Map<String,Double> scoreMap = scoreMap(k,letterScore);
    PrintWriter pw = null;
    try {
      pw = new PrintWriter(new File(name));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    Map<String,Double> treeMap = new TreeMap(scoreMap);

    for (Map.Entry<String, Double> entry : treeMap.entrySet()) {
      String key = entry.getKey().toString();
      Double value = entry.getValue();
      StringBuilder sb = new StringBuilder();
      sb.append(key);
      sb.append(',');
      sb.append(value);
      sb.append('\n');
      pw.write(sb.toString());
    }
    pw.close();
  }


}
