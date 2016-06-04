package dmatrix;

import dmatrix.io.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.lang.Runnable;
import java.lang.InterruptedException;
import java.nio.file.Paths;
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

    public static void main(String[] args) {
        String corpusRoot = args[0];
        String targetsPath = args[1];
        int dim = Integer.parseInt(args[2]);
        int numThreads = Integer.parseInt(args[3]);
        boolean getVectors = (Integer.parseInt(args[4]) == 1);
        String outputPath = args[5];
        int numRuns = Integer.parseInt(args[6]);
        Set<String> targets = loadTargets(targetsPath);
        List<Set<String>> targetPartitions = partitionTargets(targets, numRuns);
        File f = new File(Paths.get(outputPath, "vectors.txt").toString());
        if (f.exists() && !f.delete()) {
           System.out.println("Deleting previous vectors failed.");
        }
        for (Set<String> targetPartition : targetPartitions) {
            SentenceDMatrixGenerator dmg
                    = new SentenceDMatrixGenerator(corpusRoot, targetPartition, dim, numThreads, getVectors);
            dmg.generateMatrices();
            dmg.writeMatrices(outputPath);
            if (getVectors) {
                dmg.writeVectors(outputPath);
            }
            dmg.writeWordmap(outputPath);
        }
    }

    public SentenceDMatrixGenerator(String corpusRoot, Set<String> targets, int dim, int numThreads, boolean getVectors) {
        super(corpusRoot, targets, dim, numThreads, getVectors);
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

    private void updateMatrix(String target, int x, int y, float diff) {
        if (x < cutoff && y < cutoff) {
            int min = Math.min(x, y);
            int max = Math.max(x, y);
            this.densityMatrices[min][max - min].updateEntry(target, diff);
        } else if (softCutoff) {
            Pair<Integer, Integer> coords = new ImmutablePair<>(x, y);
            Map<Pair<Integer, Integer>, Float> targetMatrix = densityMatricesSparse.get(target);
            synchronized (targetMatrix) {
                targetMatrix.put(coords,
                        targetMatrix.getOrDefault(coords, 0.0f) + diff);
            }

        }
    }

    private void updateVector(String word, int x, float diff) {
        if (x < cutoff) {
            vectors[x].updateEntry(word, diff);
        }
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
        private int numProcessed;

        DMatrixFileWorker(List<String> paths) {
            this.filePaths = paths;
            tokenizedFileReaderFactory = new TokenizedFileReaderFactory();
            numProcessed = 0;
        }

        public void run() {
            for (String filePath : filePaths) {
                numProcessed++;
                System.out.println(String.format("Processing %d out of %d...", numProcessed, filePaths.size()));
                processFile(filePath);
            }
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
                    if (targets.contains(target)) {
                        if (wordMap.containsKey(target)) {
                            int targetIndex = wordMap.get(target);
                            for (Integer[] data : context) {
                                int xCount = data[0].equals(targetIndex) ? (data[1] - 1) : data[1];
                                int yCount = data[2].equals(targetIndex) ? (data[3] - 1) : data[3];
                                updateMatrix(target, data[0], data[2], xCount * yCount);
                                if (getVectors && data[0].equals(data[2])) {
                                    updateVector(target, data[0], xCount);
                                }
                            }
                        } else {
                            for (Integer[] data : context) {
                                updateMatrix(target, data[0], data[2], data[1] * data[3]);
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
