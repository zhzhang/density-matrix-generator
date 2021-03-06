package dmatrix;

import dmatrix.io.*;

import java.io.File;
import java.io.IOException;
import java.lang.Runnable;
import java.lang.InterruptedException;
import java.nio.file.Paths;
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
        String outputPath = args[5];
        int numRuns = Integer.parseInt(args[6]);
        Set<String> targets = loadTargets(targetsPath, outputPath);
        SentenceDMatrixGenerator dmg
                = new SentenceDMatrixGenerator(corpusRoot, targets, dim, numThreads, numRuns, getVectors);
        dmg.generateAndWriteMatrices(outputPath);
    }

    public SentenceDMatrixGenerator(String corpusRoot, Set<String> targets, int dim, int numThreads,
                                    int numRuns, boolean getVectors) {
        super(corpusRoot, targets, dim, numThreads, numRuns, getVectors);
    }

    public SentenceDMatrixGenerator(String corpusRoot, Set<String> targets, int dim, int numThreads,
                                    boolean getVectors) {
        super(corpusRoot, targets, dim, numThreads, 1, getVectors);
    }

    void generateWordmap(int dim) {
        System.out.println("Generating wordmap...");
        long startTime = System.nanoTime();
        TokenizedFileReaderFactory tokenizedFileReaderFactory = new TokenizedFileReaderFactory();
        WordmapGenerator wordmapGenerator
                = new WordmapGenerator(corpusRoot, tokenizedFileReaderFactory, numThreads, dim);
        wordMap = wordmapGenerator.generate();
        if (dim == 0) {
            softCutoff = true;
            cutoff = wordmapGenerator.getCutoff();
            System.out.println(String.format("Cutoff is %d out of %d", cutoff, wordMap.size()));
        } else {
            softCutoff = false;
            cutoff = dim;
        }
        System.out.println(
                String.format("Wordmap generation took %d seconds",
                        (System.nanoTime() - startTime) / 1000000000));
    }

    void generateMatricesRun() {
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
