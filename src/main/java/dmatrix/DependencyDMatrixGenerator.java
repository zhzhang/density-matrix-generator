package dmatrix;

import dmatrix.io.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Density matrix generator using sparse updates.
 * <p>
 * Created by zhuoranzhang on 4/16/16.
 */
public class DependencyDMatrixGenerator extends CountDMatrixGenerator {

    public static void main(String[] args) throws IOException {
        String corpusRoot = args[0];
        String targetsPath = args[1];
        int dim = Integer.parseInt(args[2]);
        int numThreads = Integer.parseInt(args[3]);
        boolean getVectors = (Integer.parseInt(args[4]) == 1);
        DependencyDMatrixGenerator dmg
                = new DependencyDMatrixGenerator(corpusRoot, targetsPath, dim, numThreads, getVectors);
        dmg.generateMatrices();
        String outputPath = args[5];
        dmg.writeMatrices(outputPath);
        if (getVectors) {
            dmg.writeVectors(outputPath);
        }
        dmg.writeWordmap(outputPath);
    }

    public DependencyDMatrixGenerator(String corpusRoot, String targetsPath, int dim, int numThreads, boolean getVectors) {
        super(corpusRoot, targetsPath, dim, numThreads, getVectors);
    }

    void generateWordmap(int dim) {
        DependencyWordmapGenerator dependencyWordmapGenerator
                = new DependencyWordmapGenerator(corpusRoot, targets, numThreads);
        System.out.println("Generating wordmap...");
        long startTime = System.nanoTime();
        wordMap = dependencyWordmapGenerator.generate();
        System.out.println(String.format("Wordmap generation took %d seconds",
                (System.nanoTime() - startTime) / 1000000000));
        if (dim <= 0) {
            softCutoff = true;
            cutoff = dependencyWordmapGenerator.getCutoff();
            System.out.println(String.format("Cutoff is %d", cutoff));
        } else {
            softCutoff = false;
            cutoff = dim;
        }
    }

    public void generateMatrices() throws IOException {
        System.out.println("Generating matrices...");
        long startTime = System.nanoTime();
        List<List<String>> filePathPartitions = IOUtils.getFilePathPartitions(corpusRoot, numThreads);
        ExecutorService pool = Executors.newFixedThreadPool(numThreads);
        for (List<String> filePaths : filePathPartitions) {
            pool.submit(new DMatrixSentenceWorker(filePaths, this));
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

    private class DMatrixSentenceWorker implements Runnable {
        private List<String> filePaths;
        private SentenceStreamFactory sentenceStreamFactory;

        DMatrixSentenceWorker(List<String> filePaths, DependencyDMatrixGenerator dMatrixGenerator) {
            this.filePaths = filePaths;
            this.sentenceStreamFactory = new SentenceStreamFactory(dMatrixGenerator.targets);
        }

        public void run() {
            filePaths.forEach(this::processFile);
        }

        private void processFile(String path) {
            SentenceStream sentenceStream = sentenceStreamFactory.getStream(path);
            Sentence sentence;
            while ((sentence = sentenceStream.getSentence()) != null) {
                Map<Integer, Map<String, Integer>> relationCounts = new HashMap<>(sentence.size());
                for (Integer[] dep : sentence.getDependencies()) {
                    for (int i = 0; i < 2; i++) {
                        String word1 = sentence.getWord(dep[i]);
                        String word2 = sentence.getWord(dep[(i + 1) % 2]);
                        if (targets.contains(word1)) {
                            Map<String, Integer> counts = relationCounts.get(dep[i]);
                            if (counts == null) {
                                counts = new HashMap<>();
                                relationCounts.put(dep[i], counts);
                            }
                            Integer prev = counts.get(word2);
                            if (prev == null) {
                                counts.put(word2, 1);
                            } else {
                                counts.put(word2, prev + 1);
                            }
                        }
                    }
                }
                for (Map.Entry<Integer, Map<String, Integer>> entry : relationCounts.entrySet()) {
                    updateMatrix(sentence.getWord(entry.getKey()), entry.getValue());
                    updateVector(sentence.getWord(entry.getKey()), entry.getValue());
                }
            }
            sentenceStream.close();
        }
    }

}
