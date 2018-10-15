import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Patrizio on 2017/9/20.
 */
public class Concurrent {
  // Set the folder path, and read all the files in the folder
  private static File folder = new File("input");
  private static File[] listOfFiles = folder.listFiles();
  private static int LIMIT = listOfFiles.length;
  private static int K = 2;
  private static int NTHREDS;
  private static int TIMES=10;

  public static void main(String[] args){
    NTHREDS = Integer.parseInt(args[0]);
    long res[] = new long[TIMES];
    for(int j=0;j<TIMES;j++) {
      long d1 = System.nanoTime();
      subCorpus corpus = concurrent();
      String[] ary = corpus.sub.toString().split(" ");
      int[] letterCount = corpus.letterCount;
      int totalLetter = 0;
      for (int i = 0; i < letterCount.length; i++) {
        totalLetter += letterCount[i];
      }
      int[] letterScore = NeighborhoodScore.getScore(letterCount, totalLetter);
      NeighborhoodScore neighborhoodScore = new NeighborhoodScore(ary);
      neighborhoodScore.toCSV(K,letterScore,"concurrent.csv");
      long d2 = System.nanoTime();

      res[j] = (d2 - d1) / 1000000;
    }
    PrintWriter pw = null;
    try {
      String name = "Concurrent_"+NTHREDS+"_thread.csv";
      pw = new PrintWriter(new File(name));
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



  // A subCorpus class store the store the occurrences of each letter in
  // a certain portion of the text.
  // It always has all the numbers and symbols deleted and make a clean String for further process.
  static class subCorpus{
    private int[] letterCount;
    private StringBuilder sub;
    //private int totalLetter;

    public subCorpus(int[] letterCount, StringBuilder sub){
      this.letterCount = letterCount;
      this.sub = new StringBuilder(sub);
    }
  }

  static subCorpus countLetter(int from, int to) throws IOException {
    int[] letterCount = new int[26];
    StringBuilder sb = new StringBuilder();
    int totalLetter =0;
    for (int i = from; i < to; i++) {
      FileReader fr = new FileReader(listOfFiles[i].getPath());
      BufferedReader br = new BufferedReader(fr);

      String lr = null;
      while ((lr = br.readLine()) != null) { // Go through each line
        lr = lr.replaceAll("[^a-zA-Z ]", "");
        lr = lr.toLowerCase();
        sb.append(lr);
        if (!lr.trim().isEmpty()) {  //skip empty lines
          for (int j = 0; j < lr.length(); j++) {
            Character letter = lr.charAt(j);
            if (Character.isLetter(letter)){
              letter = Character.toLowerCase(letter);
              letterCount[letter - 'a'] += 1;
            }
          }

        }
      }
      br.close();
    }
    subCorpus subCorpus = new subCorpus(letterCount,sb);
    return subCorpus;
  }

  // a Myrunnable returns an integer array which store the occurrences of each letter in
  // a certain portion of the text.
  static class MyRunnable implements Runnable{

    subCorpus subCorpus;
    int from, to;


    MyRunnable(int from, int to){
      this.from = from;
      this.to = to;
    }

    @Override
    public void run() {
      try {
        subCorpus = countLetter(from,to);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
/*
Implement the M2 solution, like professor present during class.
Manually control the thread.
 */
  static subCorpus concurrent(){
    List<Thread> threads = new ArrayList<Thread>();
    List<MyRunnable> tasks = new ArrayList<MyRunnable>();
    int from = 0;
    int segment = (LIMIT) / NTHREDS;
    for (int i = 0; i < NTHREDS; i++) {
      MyRunnable task = new MyRunnable(from, (i < NTHREDS - 1) ? from + segment : LIMIT);
      tasks.add(task);
      from += segment;
      Thread worker = new Thread(task);
      worker.start();
      threads.add(worker);
    }
    int running = 0;
    do {
      running = 0;
      for (Thread thread : threads) {
        if (thread.isAlive()) running++;
      }
    } while (running > 0);
    int[] countLetter = new int[26];
    StringBuilder corpus = new StringBuilder();
    for (MyRunnable task : tasks){
      corpus.append(task.subCorpus.sub);
      for(int j=0;j<26;j++){
      countLetter[j]+=task.subCorpus.letterCount[j];
      }
    }
    return new subCorpus(countLetter,corpus);
  }


}
