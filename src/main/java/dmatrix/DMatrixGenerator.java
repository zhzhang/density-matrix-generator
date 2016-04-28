package dmatrix;

import dmatrix.DensityMatrixDense.DMatrixListDense;
import dmatrix.DensityMatrixDense.DMatrixDense;
import dmatrix.io.IOUtils;
import dmatrix.io.TextFileReader;
import dmatrix.io.TokenizedFileReader;
import dmatrix.io.TokenizedFileReaderFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.Runnable;
import java.lang.InterruptedException;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ListIterator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DMatrixGenerator {

    // Runtime parameters.
    private String corpusRoot;
    private int numThreads;
    private int dim;
    protected Set<String> targets;
    private TokenizedFileReaderFactory tokenizedFileReaderFactory;

    protected Map<String, Integer> wordMap;
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
        this.tokenizedFileReaderFactory = new TokenizedFileReaderFactory(stopListPath);
        this.loadTargets(targetsPath);
        this.densityMatrices = new DMatrixCell[this.dim][];
        for (int i = 0; i < this.dim; i++) {
            this.densityMatrices[i] = new DMatrixCell[this.dim - i];
            for (int j = 0; j < this.dim - i; j++) {
                this.densityMatrices[i][j] = new DMatrixCell(i, j + i);
            }
        }
    }

    private void loadTargets(String targetsPath) {
        this.targets = new HashSet<String>();
        TextFileReader reader = new TextFileReader(targetsPath);
        String line;
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\\s+");
            for (String s : tmp) {
                this.targets.add(s);
            }
        }
    }

    private void generateWordmap(List<String> filePaths) {
        Map<String, Integer> counts = new HashMap<String, Integer>();
        for (String filePath : filePaths) {
            TokenizedFileReader reader = tokenizedFileReaderFactory.getReader(filePath);
            String[] tokens;
            while ((tokens = reader.readLineTokens()) != null) {
                for (String token : tokens) {
                    if (counts.containsKey(token)) {
                        counts.put(token, counts.get(token) + 1);
                    } else {
                        counts.put(token, 1);
                    }
                }
            }
        }
        List<Map.Entry<String, Integer>> sorted = counts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue()).collect(Collectors.toList());
        ListIterator<Map.Entry<String, Integer>> li = sorted.listIterator(sorted.size());
        int index = 0;
        Map<String, Integer> wordMap = new HashMap<String, Integer>();
        while (index < this.dim && li.hasPrevious()) {
            wordMap.put(li.previous().getKey(), index);
            index++;
        }
        this.wordMap = wordMap;
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
        Map<Integer, Integer> counts = new HashMap<Integer, Integer>();
        for (int index : sentence) {
            if (counts.containsKey(index)) {
                counts.put(index, counts.get(index) + 1);
            } else {
                counts.put(index, 1);
            }
        }
        List<Integer[]> countList = counts.entrySet().stream()
                .map(entry -> new Integer[]{entry.getKey(), entry.getValue()})
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
        List<String> filePaths = IOUtils.getFilePaths(this.corpusRoot);
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
        System.out.println(String.format("Matrix generation took %d seconds",
                (System.nanoTime() - startTime) / 1000000000));
    }

    protected void updateMatrix(String word, int x, int y, int diff) {
        int min;
        int max;
        if (x < y) {
            min = x;
            max = y;
        } else {
            min = y;
            max = x;
        }
        this.densityMatrices[min][max - min].updateEntry(word, diff);
    }

    public void outputMatrices(String outputPath) {
        DMatrixListDense.Builder outputList = DMatrixListDense.newBuilder();
        Map<String, DMatrixDense.Builder> outputMatrices
                = new HashMap<String, DMatrixDense.Builder>();
        for (String target : this.targets) {
            DMatrixDense.Builder targetMatrix = DMatrixDense.newBuilder();
            targetMatrix.setWord(target);
            outputMatrices.put(target, targetMatrix);
        }
        for (DMatrixCell[] tmp : this.densityMatrices) {
            for (DMatrixCell cell : tmp) {
                for (String target : this.targets) {
                    Float val = cell.getEntry(target);
                    if (val == null) {
                        outputMatrices.get(target).addData(0.0f);
                    } else {
                        outputMatrices.get(target).addData(val);
                    }
                }
            }
        }
        for (String target : this.targets) {
            outputList.addMatrices(outputMatrices.get(target));
        }
        outputList.setDimension(this.dim);
        System.out.println("made it here");
        try {
            FileOutputStream outputStream = new FileOutputStream(outputPath);
            outputList.build().writeTo(outputStream);
            outputStream.close();
        } catch (IOException e) {
        }
    }


    class DMatrixFileWorker implements Runnable {
        private List<String> filePaths;
        private DMatrixGenerator dMatrixGenerator;

        DMatrixFileWorker(List<String> paths, DMatrixGenerator dMatrixGenerator) {
            this.filePaths = paths;
            this.dMatrixGenerator = dMatrixGenerator;
        }

        public void run() {
            for (String path : filePaths) {
                this.processFile(path);
            }
        }

        public void processFile(String path) {
            TokenizedFileReader reader = tokenizedFileReaderFactory.getReader(path);
            String[] strTokens;
            while ((strTokens = reader.readLineTokens()) != null) {
                List<Integer> tokens = dMatrixGenerator.strTokensToIndices(strTokens);
                if (tokens.size() == 0) {
                    continue;
                }
                List<Integer[]> context = this.dMatrixGenerator.getContext(tokens);
                //TODO: The order of data structure usage is probably not optimal here.
                for (String target : strTokens) {
                    if (dMatrixGenerator.targets.contains(target)) {
                        if (dMatrixGenerator.wordMap.containsKey(target)) {
                            int targetIndex = dMatrixGenerator.wordMap.get(target);
                            for (Integer[] data : context) {
                                int xCount = data[0].equals(targetIndex) ? (data[1] - 1) : data[1];
                                int yCount = data[2].equals(targetIndex) ? (data[3] - 1) : data[3];
                                dMatrixGenerator.updateMatrix(target, data[0], data[2], xCount * yCount);
                            }
                        } else {
                            for (Integer[] data : context) {
                                dMatrixGenerator.updateMatrix(target, data[0], data[2], data[1] * data[3]);
                            }
                        }
                    }
                }
            }
        }

    }
}

class DMatrixCell {
    int x;
    int y;
    Map<String, Float> entries;

    DMatrixCell(int x, int y) {
        this.x = x;
        this.y = y;
        this.entries = new HashMap<String, Float>();
    }

    Float getEntry(String target) {
        return entries.get(target);
    }

    Set<Map.Entry<String, Float>> getAllEntries() {
        return entries.entrySet();
    }

    void updateEntry(String target, float diff) {
        synchronized (this) {
            Float prev = entries.get(target);
            if (prev == null) {
                entries.put(target, diff);
            } else {
                entries.put(target, prev + diff);
            }
        }
    }

}
