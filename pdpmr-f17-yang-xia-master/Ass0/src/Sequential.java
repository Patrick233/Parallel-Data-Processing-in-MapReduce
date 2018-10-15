import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by Patrizio on 2017/9/14.
 */
public class Sequential {
  // Set the folder path, and read all the files in the folder
  private static File folder = new File("input");
  private static File[] listOfFiles = folder.listFiles();
  private static int LIMIT = listOfFiles.length;
  private static int K = 2;

  public static void main(String[] args) throws IOException {
    int TIMES = 20;
    long res[] = new long[TIMES];
    for (int m = 0; m < TIMES; m++) {
      long d1 = System.nanoTime();

      //Initialized corpus, which stores all words
      StringBuilder corpus = new StringBuilder();

      //Count occurrence of each letter and store it in a array while reading all the words.
      int[] letterCount = new int[26];
      int totalLetters = 0;

      //int LIMIT = 2;
      for (int i = 0; i < LIMIT; i++) {
        FileReader fr = new FileReader(listOfFiles[i].getPath());
        BufferedReader br = new BufferedReader(fr);

        String lr = null;
        while ((lr = br.readLine()) != null) { // Go through each line
          lr = lr.replaceAll("[^a-zA-Z ]", "");
          lr = lr.toLowerCase();
          corpus.append(lr);
          if (!lr.trim().isEmpty()) {  //skip empty lines
            for (int j = 0; j < lr.length(); j++) {
              Character letter = lr.charAt(j);
              if (Character.isLetter(letter)) {
                totalLetters++;
                letter = Character.toLowerCase(letter);
                letterCount[letter - 'a'] += 1;
              }
            }

          }
        }
        br.close();
      }

      String[] ary = corpus.toString().split(" ");

      int[] letterScore = NeighborhoodScore.getScore(letterCount, totalLetters);

      NeighborhoodScore neighborhoodScore = new NeighborhoodScore(ary);
      neighborhoodScore.toCSV(K,letterScore,"sequential.csv");

      long d2 = System.nanoTime();
      res[m] = ((d2 - d1) / 1000000);
    }
    PrintWriter pw = null;
    try {
      pw = new PrintWriter(new File("SequentialResult.csv"));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    for (int i =0;i<TIMES;i++) {
      StringBuilder sb = new StringBuilder();
      sb.append(res[i]);
      sb.append('\n');
      pw.write(sb.toString());
    }
    pw.close();
  }


}