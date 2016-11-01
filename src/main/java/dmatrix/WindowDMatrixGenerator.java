package dmatrix;

import dmatrix.io.IOUtils;
import dmatrix.io.TokenizedFileReader;
import dmatrix.io.TokenizedFileReaderFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Density matrix generator using sparse updates.
 * <p>
 * Created by zhuoranzhang on 4/16/16.
 */
public class WindowDMatrixGenerator extends CountDMatrixGenerator {

    private int windowSize;

    public static void main(String[] args) {
        String corpusRoot = args[0];
        String targetsPath = args[1];
        int dim = Integer.parseInt(args[2]);
        int numThreads = Integer.parseInt(args[3]);
        boolean getVectors = (Integer.parseInt(args[4]) == 1);
        String outputPath = args[5];
        int numRuns = Integer.parseInt(args[6]);
        int windowSize = Integer.parseInt(args[7]);
        Set<String> targets = loadTargets(targetsPath, outputPath);
        WindowDMatrixGenerator dmg
                = new WindowDMatrixGenerator(corpusRoot, targets, dim, numThreads, numRuns,
                getVectors, windowSize);
        dmg.generateAndWriteMatrices(outputPath);
    }

    public WindowDMatrixGenerator(String corpusRoot, Set<String> targets, int dim, int numThreads, int numRuns,
                                  boolean getVectors, int windowSize) {
        super(corpusRoot, targets, dim, numThreads, numRuns, getVectors);
        this.windowSize = windowSize;
    }

    public WindowDMatrixGenerator(String corpusRoot, Set<String> targets, int dim, int numThreads,
                                  boolean getVectors, int windowSize) {
        super(corpusRoot, targets, dim, numThreads, 1, getVectors);
        this.windowSize = windowSize;
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

    private Map<String, Integer> getContext(String[] strTokens, int index) {
        Map<String, Integer> context = new HashMap<>();
        int diff = 1;
        String token;
        while (diff <= windowSize) {
            if (index - diff >= 0) {
                token = strTokens[index - diff];
                if (wordMap.containsKey(token)) {
                    context.put(token, context.getOrDefault(token, 0) + 1);
                }
            }
            if (diff + index < strTokens.length) {
                token = strTokens[index + diff];
                if (wordMap.containsKey(token)) {
                    context.put(token, context.getOrDefault(token, 0) + 1);
                }
            }
            diff++;
        }
        return context;
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
            String token;
            Map<String, Integer> context;
            while ((strTokens = reader.readLineTokens()) != null) {
                for (int i = 0; i < strTokens.length; i++) {
                    token = strTokens[i];
                    if (targets.contains(token)) {
                        context = getContext(strTokens, i);
                        updateMatrix(token, context);
                        updateVector(token, context);
                    }
                }
            }
            reader.close();
        }

    }

}
