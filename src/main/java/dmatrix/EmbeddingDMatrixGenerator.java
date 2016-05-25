package dmatrix;

import dmatrix.io.*;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class EmbeddingDMatrixGenerator {

    // Runtime parameters.
    private String corpusRoot;
    private int numThreads;
    private int dim;
    private Set<String> targets;
    private TokenizedFileReaderFactory tokenizedFileReaderFactory;

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
        this.corpusRoot = corpusRoot;
        this.numThreads = numThreads;
        this.tokenizedFileReaderFactory = new TokenizedFileReaderFactory();
        this.loadTargets(targetsPath);
        WordmapGenerator wordmapGenerator
                = new WordmapGenerator(corpusRoot, tokenizedFileReaderFactory, numThreads, numContexts);
        this.wordMap = wordmapGenerator.generate(vectorsPath);
        String tmpTarget = wordMap.keySet().iterator().next();
        this.dim = wordMap.get(tmpTarget).length;
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

    protected float[] getContext(String[] tokens) {
        float[] output = new float[this.dim];
        float[] tokenVector;
        for (String token : tokens) {
            tokenVector = wordMap.get(token);
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
        List<List<String>> filePathPartitions = IOUtils.getFilePathPartitions(corpusRoot, numThreads);
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
        float norm = 0.0f;
        for (float val : context) {
            norm += val * val;
        }
        if (norm == 0.0f) {
            return;
        }
        float[][] prev = densityMatrices.get(target);
        synchronized (prev) {
            for (int i = 0; i < this.dim; i++) {
                for (int j = 0; j < this.dim - i; j++) {
                    prev[i][j] += context[i] * context[i + j] / norm;
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
            for (int j = 0; j < dim - i; j++) {
                output[i][i + j] = output[i + j][i] = data[i][j];
            }
        }
        return output;
    }


    public void writeMatrices(String outputPath) {
        for (String target : targets) {
            float[][] matrix = densityMatrices.get(target);
            outer:
            for (float[] row : matrix) {
                for (float value : row) {
                    if (value != 0.0) {
                        DenseDMatrixWriter writer = new DenseDMatrixWriter(target, outputPath);
                        writer.writeMatrix(matrix);
                        writer.close();
                        break outer;
                    }
                }
            }
        }
        try {
            PrintWriter writer = new PrintWriter(Paths.get(outputPath, "parameters.txt").toString());
            writer.println(String.format("%s %d", "dimension", dim));
            writer.flush();
            writer.close();
        } catch (FileNotFoundException e) {
            System.out.println(String.format("File %s could not be created", outputPath));
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
            reader.close();
        }
    }


}
