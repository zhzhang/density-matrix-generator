package dmatrix;

import dmatrix.io.*;

import java.lang.Runnable;
import java.lang.InterruptedException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Density matrix generator using sparse updates.
 * <p>
 * Created by zhuoranzhang on 4/16/16.
 */
public class SentenceDMatrixGenerator extends CountDMatrixGenerator {

    private Map<String, Integer> wordMap;

    public static void main(String[] args) {
        String corpusRoot = args[0];
        String targetsPath = args[1];
        int dim = Integer.parseInt(args[2]);
        int numThreads = Integer.parseInt(args[3]);
        boolean getVectors = (Integer.parseInt(args[5]) == 1);
        SentenceDMatrixGenerator dmg = new SentenceDMatrixGenerator(corpusRoot, targetsPath, dim, numThreads, getVectors);
        dmg.generateMatrices();
        dmg.writeMatrices(args[4]);
        if (getVectors) {
            dmg.writeVectors(args[4]);
        }
        dmg.writeWordmap(args[4]);
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

    private void updateMatrix(String word, int x, int y, float diff) {
        int min = Math.min(x, y);
        int max = Math.max(x, y);
        this.densityMatrices[min][max - min].updateEntry(word, diff);
    }

    private void updateVector(String word, int x, float diff) {
        vectors[x].updateEntry(word, diff);
    }

    private class DMatrixFileWorker implements Runnable {
        private List<String> filePaths;
        private SentenceDMatrixGenerator dMatrixGenerator;
        private TokenizedFileReaderFactory tokenizedFileReaderFactory;

        DMatrixFileWorker(List<String> paths, SentenceDMatrixGenerator dMatrixGenerator) {
            this.filePaths = paths;
            this.dMatrixGenerator = dMatrixGenerator;
            tokenizedFileReaderFactory = new TokenizedFileReaderFactory();
        }

        public void run() {
            filePaths.forEach(this::processFile);
        }

        void processFile(String path) {
            TokenizedFileReader reader = tokenizedFileReaderFactory.getReader(path);
            String[] strTokens;
            while ((strTokens = reader.readLineTokens()) != null) {
                List<Integer> tokens = strTokensToIndices(strTokens);
                if (tokens.size() == 0) {
                    continue;
                }
                List<Integer[]> context = getContext(tokens);
                //TODO: The order of data structure usage is probably not optimal here.
                for (String target : strTokens) {
                    if (dMatrixGenerator.targets.contains(target)) {
                        if (dMatrixGenerator.wordMap.containsKey(target)) {
                            int targetIndex = dMatrixGenerator.wordMap.get(target);
                            for (Integer[] data : context) {
                                int xCount = data[0].equals(targetIndex) ? (data[1] - 1) : data[1];
                                int yCount = data[2].equals(targetIndex) ? (data[3] - 1) : data[3];
                                dMatrixGenerator.updateMatrix(target, data[0], data[2], xCount * yCount);
                                if (getVectors && data[0].equals(data[2])) {
                                    dMatrixGenerator.updateVector(target, data[0], xCount);
                                }
                            }
                        } else {
                            for (Integer[] data : context) {
                                dMatrixGenerator.updateMatrix(target, data[0], data[2], data[1] * data[3]);
                            }
                        }
                    }
                }
            }
            reader.close();
        }

        private List<Integer[]> getContext(List<Integer> sentence) {
            Map<Integer, Integer> counts = new HashMap<>();
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
            List<Integer[]> output = new ArrayList<>();
            int index = 0;
            for (Integer[] current : countList) {
                ListIterator<Integer[]> innerIter = countList.listIterator(index);
                while (innerIter.hasNext()) {
                    Integer[] tmp = innerIter.next();
                    output.add(new Integer[]{current[0], current[1], tmp[0], tmp[1]});
                }
                index++;
            }
            return output;
        }

        private List<Integer> strTokensToIndices(String[] strTokens) {
            List<Integer> output = new ArrayList<>();
            for (String token : strTokens) {
                if (wordMap.containsKey(token)) {
                    output.add(wordMap.get(token));
                }
            }
            return output;
        }


    }

}
