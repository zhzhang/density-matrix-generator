package dmatrix;

import dmatrix.DMatrixProtos.DMatrixDense;
import dmatrix.io.IOUtils;
import dmatrix.io.TextFileReader;
import dmatrix.io.TokenizedFileReader;
import dmatrix.io.TokenizedFileReaderFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class EmbeddingDMatrixGenerator {

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
        String corpusRoot = args[0];
        String targetsPath = args[1];
        int numContexts = Integer.parseInt(args[2]);
        String vectorsPath = args[3];
        int numThreads = Integer.parseInt(args[4]);
        String outputPath = args[5];
        EmbeddingDMatrixGenerator dmg = new EmbeddingDMatrixGenerator(corpusRoot, targetsPath, numContexts,
                vectorsPath, numThreads);
        dmg.generateMatrices();
        dmg.writeMatrices(outputPath);
    }

    public EmbeddingDMatrixGenerator(String corpusRoot, String targetsPath, int numContexts,
                                     String vectorsPath, int numThreads) {
        this.numThreads = numThreads;
        this.numContexts = numContexts;
        this.tokenizedFileReaderFactory = new TokenizedFileReaderFactory();
        this.loadTargets(targetsPath);
        this.filePathPartitions = IOUtils.getFilePathPartitions(corpusRoot, numThreads);
        this.generateWordmap(vectorsPath);
        this.densityMatrices = new HashMap<>();
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
        Map<String, Integer> counts = new HashMap<>();

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

    private void updateMatrix(String target, float[] baseContext) {
        float[] context = Arrays.copyOf(baseContext, baseContext.length);
        float[] targetVector = wordMap.get(target);
        if (targetVector != null) {
            for (int i = 0; i < baseContext.length; i++) {
                context[i] -= targetVector[i];
            }
        }
        float[][] prev = densityMatrices.get(target);
        synchronized (prev) {
            for (int i = 0; i < this.dim; i++) {
                for (int j = 0; j < this.dim - i; j++) {
                    prev[i][j] += context[i] * context[i + j];
                }
            }
        }
    }

    public float[][] getMatrix(String target) {
        float[][] output = new float[dim][dim];
        float[][] data = densityMatrices.get(target);
        if (data == null) {
            return null;
        }
        for (int i = 0; i < dim; i++) {
            for (int j = 0; j < dim-i; j++) {
                output[i][i+j] = output[i+j][i] = data[i][j];
            }
        }
        return output;
    }


    public void writeMatrices(String outputPath) {
        for (String target : targets) {
            DMatrixDense.Builder targetMatrix = DMatrixDense.newBuilder();
            targetMatrix.setWord(target);
            targetMatrix.setDimension(dim);
            for (float[] tmp : densityMatrices.get(target)) {
                for (float val : tmp) {
                    targetMatrix.addData(val);
                }
            }
            try {
                FileOutputStream outputStream = IOUtils.getOutputStream(outputPath, target);
                targetMatrix.build().writeTo(outputStream);
                outputStream.close();
            } catch (IOException e) {
                System.out.println("Failed to write matrices to output.");
            }
        }
    }

    private class DMatrixFileWorkerDense implements Runnable {
        private List<String> paths;
        private EmbeddingDMatrixGenerator dMatrixGenerator;

        DMatrixFileWorkerDense(List<String> paths, EmbeddingDMatrixGenerator dMatrixGenerator) {
            this.paths = paths;
            this.dMatrixGenerator = dMatrixGenerator;
        }

        public void run() {
            for (String path : paths) {
                this.processFile(path);
            }
        }

        private void processFile(String path) {
            TokenizedFileReader reader = dMatrixGenerator.tokenizedFileReaderFactory.getReader(path);
            String[] tokens;
            while ((tokens = reader.readLineTokens()) != null) {
                if (tokens.length == 0)
                    continue;
                float[] baseContext = dMatrixGenerator.getContext(tokens);
                for (String target : tokens) {
                    if (dMatrixGenerator.targets.contains(target)) {
                        dMatrixGenerator.updateMatrix(target, baseContext);
                    }
                }
            }
        }
    }

    private class WordMapFileWorkerDense implements Runnable {
        private List<String> paths;
        private EmbeddingDMatrixGenerator dMatrixGenerator;
        private final Map<String, Integer> totalCounts;

        WordMapFileWorkerDense(List<String> paths, EmbeddingDMatrixGenerator dMatrixGenerator, Map<String, Integer> totalCounts) {
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

        private void updateCounts(Map<String, Integer> diff) {
            synchronized (totalCounts) {
                for (Map.Entry<String, Integer> entry : diff.entrySet()) {
                    String target = entry.getKey();
                    Integer prev = totalCounts.get(target);
                    if (prev == null) {
                        totalCounts.put(target, entry.getValue());
                    } else {
                        totalCounts.put(target, prev + entry.getValue());
                    }
                }
            }
        }
    }

}
