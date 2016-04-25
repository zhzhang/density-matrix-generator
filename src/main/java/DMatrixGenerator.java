import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.lang.Runnable;
import java.lang.InterruptedException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ListIterator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import dmatrix.DensityMatrix.DMatrixList;
import dmatrix.DensityMatrix.DMatrix;
import java.io.FileInputStream;

public class DMatrixGenerator {

  // Runtime parameters.
  private String corpusRoot;
  private int numThreads;
  private int dim;
  protected Set<String> targets;
  private Set<String> stopWords;

  protected Map<String,Integer> wordMap;
  private DMatrixCell[][] densityMatrices;

  public static void main(String[] args) {
    DMatrixGenerator dmg = new DMatrixGenerator(args[0], args[1],
        Integer.parseInt(args[2]), Integer.parseInt(args[3]), args[4]);
    dmg.generateMatrices();
    dmg.outputMatrices(args[5]);
  }

  DMatrixGenerator(String corpusRoot, String targetsPath,
      int dim, int numThreads, String stopListPath) {
    this.corpusRoot = corpusRoot;
    this.numThreads = numThreads;
    this.dim = dim;
    this.loadStopList(stopListPath);
    this.loadTargets(targetsPath);
    this.densityMatrices = new DMatrixCell[this.dim][];
    for (int i = 0; i < this.dim; i++) {
      this.densityMatrices[i] = new DMatrixCell[this.dim - i];
      for (int j = 0; j < this.dim - i; j++) {
        this.densityMatrices[i][j] = new DMatrixCell(i,j);
      }
    }
  }

  private void loadStopList(String stopListPath) {
    BufferedReader br = this.getReader(stopListPath);
    Set<String> stopWords = new HashSet<String>();
    try {
      String word;
      while ( (word = br.readLine()) != null) {
        stopWords.add(word);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    this.stopWords = stopWords;
  }

  private void loadTargets(String targetsPath) {
    this.targets = new HashSet<String>();
    BufferedReader br = this.getReader(targetsPath);
    try {
      String line;
      while ( (line = br.readLine()) != null) {
        String[] tmp = line.split("\\s+");
        if (tmp[2].equals("hyper")) {
          this.targets.add(tmp[0].split("-")[0]);
          this.targets.add(tmp[3].split("-")[0]);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static List<String> getFilePaths(String path) {
    File f = new File(path);
    if (f.isFile()) {
      return Arrays.asList(path);
    } else if (f.isDirectory()) {
      List<String> result = new ArrayList<String>();
      for (File child : f.listFiles()) {
        result.addAll(getFilePaths(child.getPath()));
      }
      return result;
    }
    return null;
  }

  private void generateWordmap(List<String> filePaths) {
    Map<String,Integer> counts = new HashMap<String,Integer>();
    for (String filePath : filePaths) {
      BufferedReader br = getReader(filePath);
      String line;
      try {
        while ( (line = br.readLine()) != null) {
          String[] tokens = this.tokenizeLine(line);
          for (String token : tokens) {
            if (counts.containsKey(token)) {
              counts.put(token, counts.get(token) + 1);
            } else {
              counts.put(token, 1);
            }
          }
        }
      } catch (IOException e) {
      }
    }
    List<Map.Entry<String,Integer>> sorted = counts.entrySet().stream()
      .sorted(Map.Entry.comparingByValue()).collect(Collectors.toList());
    ListIterator<Map.Entry<String,Integer>> li = sorted.listIterator(sorted.size());
    int index = 0;
    Map<String,Integer> wordMap = new HashMap<String,Integer>();
    while (index < this.dim && li.hasPrevious()) {
      wordMap.put(li.previous().getKey(), index);
      index++;
    }
    this.wordMap = wordMap;
  }

  private static String getFileExtension(String fileName) {
    if (fileName.lastIndexOf(".") != -1 && fileName.lastIndexOf(".") != 0)
      return fileName.substring(fileName.lastIndexOf(".")+1);
    else return "";
  }

  protected BufferedReader getReader(String filePath) {
    try {
      BufferedReader br;
      if (getFileExtension(filePath).equals("gz")) {
        GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(filePath));
        br = new BufferedReader(new InputStreamReader(gzip));
      } else {
        br = new BufferedReader(new FileReader(filePath));
      }
      return br;
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  public String[] tokenizeLine(String line) {
    if (line.startsWith("<doc") || line.startsWith("</doc")) {
      return new String[0];
    }
    String[] tokens = line.replaceAll("[^a-zA-Z\\s]", "").toLowerCase().split("\\s+");
    List<String> output = new ArrayList<String>();
    for (String token : tokens) {
      if (this.stopWords != null && !this.stopWords.contains(token)) {
        output.add(token);
      }
    }
    return output.toArray(new String[output.size()]);
  }

  protected List<Integer> strTokensToIndices(String[] strTokens) {
    List<Integer> output = new ArrayList<Integer>();
    for (int i = 0; i < strTokens.length; i++) {
      if (this.wordMap.containsKey(strTokens[i])) {
        output.add(this.wordMap.get(strTokens[i]));
      }
    }
    return output;
  }

  protected List<Integer[]> getContext(List<Integer> sentence) {
    Map<Integer,Integer> counts = new HashMap<Integer,Integer>();
    for (int index : sentence) {
      if (counts.containsKey(index)) {
        counts.put(index, counts.get(index) + 1);
      } else {
        counts.put(index, 1);
      }
    }
    List<Integer[]> countList = counts.entrySet().stream()
      .map(entry->new Integer[]{entry.getKey(),entry.getValue()})
      .collect(Collectors.toList());
    List<Integer[]> output = new ArrayList<Integer[]>();
    int index = 0;
    ListIterator<Integer[]> mainIter = countList.listIterator();
    while (mainIter.hasNext()) {
      Integer[] current = mainIter.next();
      ListIterator<Integer[]> innerIter = countList.listIterator(index);
      while (innerIter.hasNext()) {
        Integer[] tmp = innerIter.next();
        output.add(new Integer[]{current[0], current[1], tmp[0], tmp[1]});
      }
      index++;
    }
    return output;
  }

  public void generateMatrices() {
    List<String> filePaths = getFilePaths(this.corpusRoot);
    int partitionSize = (int) Math.ceil((float) filePaths.size() / this.numThreads);
    List<List<String>> filePathPartitions = new ArrayList<List<String>>();
    for (int i = 0; i < filePaths.size(); i += partitionSize) {
      filePathPartitions.add(filePaths.subList(i, Math.min(i + partitionSize, filePaths.size())));
    }
    System.out.println("Generating wordmap...");
    long startTime = System.nanoTime();
    generateWordmap(filePaths);
    System.out.println(
      String.format("Wordmap generation took %d seconds",
      (System.nanoTime() - startTime) / 1000000000));
    System.out.println("Generating matrices...");
    startTime = System.nanoTime();
    ExecutorService pool = Executors.newFixedThreadPool(this.numThreads);
    for (List<String> filePathPartition : filePathPartitions) {
      pool.submit(new DMatrixFileWorker(filePathPartition, this));
    }
    pool.shutdown();
    try {
      pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    } catch (InterruptedException e) {
      //TODO: see if graceful exit is possible here
    }
    System.out.println(
      String.format("Matrix generation took %d seconds",
      (System.nanoTime() - startTime) / 1000000000));
  }

  protected void updateMatrix(String word, int x, int y, int diff) {
    int min; int max;
    if (x < y) {
      min = x; max = y;
    } else {
      min = y; max = x;
    }
    this.densityMatrices[min][max-min].updateEntry(word, diff);
  }

  public void outputMatrices(String outputPath) {
    DMatrixList.Builder outputList = DMatrixList.newBuilder();
    Map<String,DMatrix.Builder> outputMatrices
        = new HashMap<String,DMatrix.Builder>();
    for (String target : this.targets) {
      DMatrix.Builder targetMatrix = DMatrix.newBuilder();
      targetMatrix.setWord(target);
      outputMatrices.put(target, targetMatrix);
    }
    for (DMatrixCell[] tmp : this.densityMatrices) {
      for (DMatrixCell cell : tmp) {
        for (Map.Entry<String,Float> entry : cell.getAllEntries()) {
          DMatrix.DMatrixEntry.Builder dMatrixEntry
              = DMatrix.DMatrixEntry.newBuilder();
          dMatrixEntry.setX(cell.x).setY(cell.y).setVal(entry.getValue());
          outputMatrices.get(entry.getKey()).addEntries(dMatrixEntry.build());
        }
      }
    }
    for (String target : this.targets) {
      outputList.addMatrices(outputMatrices.get(target));
    }
    try {
      FileOutputStream outputStream = new FileOutputStream("matrices.dat");
      outputList.build().writeTo(outputStream);
      outputStream.close();
    } catch (IOException e) {
    }
  }

}

class DMatrixFileWorker implements Runnable {
  private List<String> paths;
  private DMatrixGenerator dMatrixGenerator;

  DMatrixFileWorker(List<String> paths, DMatrixGenerator dMatrixGenerator) {
    this.paths = paths;
    this.dMatrixGenerator = dMatrixGenerator;
  }

  public void run() {
    for (String path : this.paths) {
      this.processFile(path);
    }
  }

  public void processFile(String path) {
    BufferedReader fileReader = this.dMatrixGenerator.getReader(path);
    System.out.print("Processing file ");
    System.out.println(path);
    try {
      String line;
      while ( (line = fileReader.readLine()) != null) {
        String[] strTokens = this.dMatrixGenerator.tokenizeLine(line);
        List<Integer> tokens = this.dMatrixGenerator.strTokensToIndices(strTokens);
        if (tokens.size() == 0) {
          continue;
        }
        List<Integer[]> context = this.dMatrixGenerator.getContext(tokens);
        //TODO: The order of data structure usage is probably not optimal here.
        for (String target : strTokens) {
          if (this.dMatrixGenerator.targets.contains(target)) {
            if (this.dMatrixGenerator.wordMap.containsKey(target)) {
              int targetIndex = this.dMatrixGenerator.wordMap.get(target);
              for (Integer[] data : context) {
                int xCount = data[0].equals(targetIndex) ? data[1] : (data[1]-1);
                int yCount = data[2].equals(targetIndex) ? data[3] : (data[3]-1);
                this.dMatrixGenerator.updateMatrix(target, data[0], data[2], xCount * yCount);
              }
            } else {
              for (Integer[] data : context) {
                this.dMatrixGenerator.updateMatrix(target, data[0], data[2], data[1] * data[3]);
              }
            }
          }
        }
      }
    } catch (IOException e) {
      System.out.println("IOException in run.");
    }
  }

}

class DMatrixCell {
  int x;
  int y;
  Map<String,Float> entries;

  DMatrixCell(int x, int y) {
    this.x = x;
    this.y = y;
    this.entries = new HashMap<String,Float>();
  }

  float getEntry(String target) {
    return this.entries.get(target);
  }

  Set<Map.Entry<String,Float>> getAllEntries() {
    return this.entries.entrySet();
  }

  void updateEntry(String target, float diff) {
    synchronized(this) {
      if (this.entries.containsKey(target)) {
        this.entries.put(target, this.entries.get(target) + diff);
      } else {
        this.entries.put(target, diff);
      }
    }
  }

}
