package dmatrix;

import dmatrix.io.*;

import java.lang.Runnable;
import java.lang.InterruptedException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Density matrix generator using sparse updates.
 * <p>
 * Created by zhuoranzhang on 4/16/16.
 */
public class SentenceDMatrixGenerator extends CountDMatrixGenerator {

    public static void main(String[] args) {
        String corpusRoot = args[0];
        String targetsPath = args[1];
        int dim = Integer.parseInt(args[2]);
        int numThreads = Integer.parseInt(args[3]);
        boolean getVectors = (Integer.parseInt(args[4]) == 1);
        SentenceDMatrixGenerator dmg
                = new SentenceDMatrixGenerator(corpusRoot, targetsPath, dim, numThreads, getVectors);
        dmg.generateMatrices();
        String outputPath = args[5];
        dmg.writeMatrices(outputPath);
        if (getVectors) {
            dmg.writeVectors(outputPath);
        }
        dmg.writeWordmap(outputPath);
    }

    public SentenceDMatrixGenerator(String corpusRoot, String targetsPath, int dim, int numThreads, boolean getVectors) {
        super(corpusRoot, targetsPath, dim, numThreads, getVectors);
    }

    void generateWordmap(int dim) {
        System.out.println("Generating wordmap...");
        long startTime = System.nanoTime();
        TokenizedFileReaderFactory tokenizedFileReaderFactory = new TokenizedFileReaderFactory();
        WordmapGenerator wordmapGenerator
                = new WordmapGenerator(corpusRoot, tokenizedFileReaderFactory, numThreads, dim);
        wordMap = wordmapGenerator.generate();
        cutoff = dim;
        System.out.println(
                String.format("Wordmap generation took %d seconds",
                        (System.nanoTime() - startTime) / 1000000000));
    }

    public void generateMatrices() {
        // Generate matrices.
        System.out.println("Generating matrices...");
        long startTime = System.nanoTime();
        List<List<String>> filePathPartitions = IOUtils.getFilePathPartitions(corpusRoot, numThreads);
        ExecutorService pool = Executors.newFixedThreadPool(this.numThreads);
        for (List<String> filePathPartition : filePathPartitions) {
            pool.submit(new DMatrixFileWorker(filePathPartition));
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

    private class DMatrixFileWorker implements Runnable {
        private List<String> filePaths;
        private TokenizedFileReaderFactory tokenizedFileReaderFactory;

        DMatrixFileWorker(List<String> paths) {
            this.filePaths = paths;
            tokenizedFileReaderFactory = new TokenizedFileReaderFactory();
        }

        public void run() {
            filePaths.forEach(this::processFile);
        }

        void processFile(String path) {
            TokenizedFileReader reader = tokenizedFileReaderFactory.getReader(path);
            String[] strTokens;
            while ((strTokens = reader.readLineTokens()) != null) {
                Map<String, Integer> context = new HashMap<>();
                for (String token : strTokens) {
                    if (wordMap.containsKey(token)) {
                        context.put(token, context.getOrDefault(token, 0) + 1);
                    }
                }
                for (String token : strTokens) {
                    if (targets.contains(token)) {
                        if (wordMap.containsKey(token) && context.containsKey(token)) {
                            context.put(token, context.get(token) - 1);
                            updateMatrix(token, context);
                            updateVector(token, context);
                            context.put(token, context.get(token) + 1);
                        } else {
                            updateMatrix(token, context);
                            updateVector(token, context);
                        }
                    }
                }
            }
            reader.close();
        }

    }

}
