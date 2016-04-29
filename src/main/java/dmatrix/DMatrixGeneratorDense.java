package dmatrix;

import dmatrix.DensityMatrixDense.DMatrixListDense;
import dmatrix.DensityMatrixDense.DMatrixDense;
import dmatrix.io.IOUtils;
import dmatrix.io.TextFileReader;
import dmatrix.io.TokenizedFileReader;
import dmatrix.io.TokenizedFileReaderFactory;

import java.io.*;
import java.lang.Runnable;
import java.lang.InterruptedException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DMatrixGeneratorDense {

    // Runtime parameters.
    private int numThreads;
    private int dim;
    private int numContexts;
    private Set<String> targets;
    private TokenizedFileReaderFactory tokenizedFileReaderFactory;

    private List<List<String>> filePathPartitions;
    private Map<String, float[]> wordMap;
    private Map<String, float[][]> densityMatrices;

    public static void main(String[] args) {
        DMatrixGeneratorDense dmg = new DMatrixGeneratorDense(args[0], args[1],
                Integer.parseInt(args[2]),
                args[3], Integer.parseInt(args[4]), args[5]);
        dmg.generateMatrices();
        dmg.outputMatrices(args[6]);
    }

    DMatrixGeneratorDense(String corpusRoot, String targetsPath, int numContexts,
                          String vectorsPath, int numThreads, String stopListPath) {
        this.numThreads = numThreads;
        this.numContexts = numContexts;
        this.tokenizedFileReaderFactory = new TokenizedFileReaderFactory(stopListPath);
        this.loadTargets(targetsPath);
        this.filePathPartitions = IOUtils.getFilePathPartitions(corpusRoot, numThreads);
        this.generateWordmap(vectorsPath);
        this.densityMatrices = new HashMap<String, float[][]>();
        for (String target : this.targets) {
            float[][] initialMatrix = new float[this.dim][];
            for (int i = 0; i < this.dim; i++) {
                initialMatrix[i] = new float[this.dim - i];
            }
            this.densityMatrices.put(target, initialMatrix);
        }
    }

    private void loadTargets(String targetsPath) {
        /**
         * Loads target words from text file.
         * @targetsPath path to target words text file
         **/
        this.targets = new HashSet<>();
        TextFileReader reader = new TextFileReader(targetsPath);
        String line;
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\\s+");
            for (String s : tmp) {
                this.targets.add(s);
            }
        }
    }

    private void generateWordmap(String vectorsPath) {
        System.out.println("Generating wordmap...");
        long startTime = System.nanoTime();
        Map<String, Integer> counts = new HashMap<String, Integer>();

        ExecutorService pool = Executors.newFixedThreadPool(this.numThreads);
        for (List<String> filePathPartition : this.filePathPartitions) {
            pool.submit(new WordMapFileWorkerDense(filePathPartition, this, counts));
        }
        pool.shutdown();
        try {
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            //TODO: see if graceful exit is possible here
        }

        List<Map.Entry<String, Integer>> sorted = counts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue()).collect(Collectors.toList());
        ListIterator<Map.Entry<String, Integer>> li = sorted.listIterator(sorted.size());
        int index = 0;
        Set<String> topN = new HashSet<>();
        while (index < this.numContexts && li.hasPrevious()) {
            topN.add(li.previous().getKey());
            index++;
        }
        TextFileReader reader = new TextFileReader(vectorsPath);
        Map<String, float[]> wordMap = new HashMap<>();
        String line;
        float[] vector = new float[0];
        while ((line = reader.readLine()) != null) {
            String[] tokens = line.split("\\s+");
            if (topN.contains(tokens[0])) {
                vector = new float[tokens.length - 1];
                for (int i = 1; i < tokens.length; i++) {
                    vector[i - 1] = Float.parseFloat(tokens[i]);
                }
                wordMap.put(tokens[0], vector);
            }
        }
        this.dim = vector.length;
        this.wordMap = wordMap;
        System.out.println(String.format("Wordmap generation took %d seconds",
                (System.nanoTime() - startTime) / 1000000000));
    }

    protected float[] getContext(String[] tokens) {
        float[] output = new float[this.dim];
        float[] tokenVector;
        for (String token : tokens) {
            tokenVector = this.wordMap.get(token);
            if (tokenVector == null) continue;
            for (int i = 0; i < output.length; i++) {
                output[i] += tokenVector[i];
            }
        }
        return output;
    }

    public void generateMatrices() {
        System.out.println("Generating matrices...");
        long startTime = System.nanoTime();
        ExecutorService pool = Executors.newFixedThreadPool(this.numThreads);
        for (List<String> filePathPartition : filePathPartitions) {
            pool.submit(new DMatrixFileWorkerDense(filePathPartition, this));
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

    protected void updateMatrix(String word, float[] context) {
        float[][] prev = this.densityMatrices.get(word);
        synchronized (prev) {
            for (int i = 0; i < this.dim; i++) {
                for (int j = 0; j < this.dim - i; j++) {
                    prev[i][j] += context[i] * context[i + j];
                }
            }
        }
    }

    public void outputMatrices(String outputPath) {
        DMatrixListDense.Builder outputList = DMatrixListDense.newBuilder();
        for (String target : targets) {
            DMatrixDense.Builder targetMatrix = DMatrixDense.newBuilder();
            targetMatrix.setWord(target);
            for (float[] tmp : densityMatrices.get(target)) {
                for (float val : tmp) {
                    targetMatrix.addData(val);
                }
            }
            outputList.addMatrices(targetMatrix);
        }
        outputList.setDimension(dim);
        try {
            FileOutputStream outputStream = new FileOutputStream(outputPath);
            outputList.build().writeTo(outputStream);
            outputStream.close();
        } catch (IOException e) {
            System.out.println("Failed to write matrices to output.");
        }
    }

    class DMatrixFileWorkerDense implements Runnable {
        private List<String> paths;
        private DMatrixGeneratorDense dMatrixGenerator;

        DMatrixFileWorkerDense(List<String> paths, DMatrixGeneratorDense dMatrixGenerator) {
            this.paths = paths;
            this.dMatrixGenerator = dMatrixGenerator;
        }

        public void run() {
            for (String path : this.paths) {
                this.processFile(path);
            }
        }

        public void processFile(String path) {
            TokenizedFileReader reader = dMatrixGenerator.tokenizedFileReaderFactory.getReader(path);
            String[] tokens;
            while ((tokens = reader.readLineTokens()) != null) {
                if (tokens.length == 0)
                    continue;
                float[] context = this.dMatrixGenerator.getContext(tokens);
                for (String target : tokens) {
                    if (dMatrixGenerator.targets.contains(target))
                        this.dMatrixGenerator.updateMatrix(target, context);
                }
            }
        }
    }

    class WordMapFileWorkerDense implements Runnable {
        private List<String> paths;
        private DMatrixGeneratorDense dMatrixGenerator;
        private Map<String, Integer> totalCounts;

        WordMapFileWorkerDense(List<String> paths, DMatrixGeneratorDense dMatrixGenerator, Map<String, Integer> totalCounts) {
            this.paths = paths;
            this.dMatrixGenerator = dMatrixGenerator;
            this.totalCounts = totalCounts;
        }

        public void run() {
            for (String path : this.paths) {
                Map<String, Integer> counts = new HashMap<String, Integer>();
                TokenizedFileReader reader = dMatrixGenerator.tokenizedFileReaderFactory.getReader(path);
                String[] tokens;
                while ((tokens = reader.readLineTokens()) != null) {
                    for (String token : tokens) {
                        Integer prev = counts.get(token);
                        if (prev == null) {
                            counts.put(token, 1);
                        } else {
                            counts.put(token, prev + 1);
                        }
                    }
                }
                this.updateCounts(counts);
            }
        }

        public void updateCounts(Map<String, Integer> diff) {
            synchronized (this.totalCounts) {
                for (Map.Entry<String, Integer> entry : diff.entrySet()) {
                    String target = entry.getKey();
                    Integer prev = this.totalCounts.get(target);
                    if (prev == null) {
                        this.totalCounts.put(target, entry.getValue());
                    } else {
                        this.totalCounts.put(target, prev + entry.getValue());
                    }
                }
            }
        }
    }

}
