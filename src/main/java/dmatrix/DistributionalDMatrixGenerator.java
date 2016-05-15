package dmatrix;

import dmatrix.io.*;

import java.io.*;
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
public class DistributionalDMatrixGenerator {

    // Runtime parameters.
    private String corpusRoot;
    private int numThreads;
    private int dim;
    private boolean getVectors;
    private Set<String> targets;
    private TokenizedFileReaderFactory tokenizedFileReaderFactory;

    private Map<String, Integer> wordMap;
    private DataCell[][] densityMatrices;
    private DataCell[] vectors;

    public static void main(String[] args) {
        String corpusRoot = args[0];
        String targetsPath = args[1];
        int dim = Integer.parseInt(args[2]);
        int numThreads = Integer.parseInt(args[3]);
        boolean getVectors = (Integer.parseInt(args[5]) == 1);
        DistributionalDMatrixGenerator dmg = new DistributionalDMatrixGenerator(corpusRoot, targetsPath, dim, numThreads, getVectors);
        dmg.generateMatrices();
        dmg.writeMatrices(args[4]);
        if (getVectors) {
            dmg.writeVectors(args[4]);
        }
        dmg.writeWordmap(args[4]);
    }

    public DistributionalDMatrixGenerator(String corpusRoot, String targetsPath, int dim, int numThreads, boolean getVectors) {
        this.corpusRoot = corpusRoot;
        this.numThreads = numThreads;
        this.dim = dim;
        this.getVectors = getVectors;
        this.tokenizedFileReaderFactory = new TokenizedFileReaderFactory();
        this.loadTargets(targetsPath);
        this.densityMatrices = new DataCell[dim][];
        this.vectors = new DataCell[dim];
        for (int i = 0; i < dim; i++) {
            this.densityMatrices[i] = new DataCell[dim - i];
            if (getVectors) {
                this.vectors[i] = new DataCell();
            }
            for (int j = 0; j < dim - i; j++) {
                this.densityMatrices[i][j] = new DataCell();
            }
        }
    }

    private void loadTargets(String targetsPath) {
        targets = new HashSet<>();
        TextFileReader reader = new TextFileReader(targetsPath);
        String line;
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\\s+");
            for (String s : tmp) {
                targets.add(s.toLowerCase());
            }
        }
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

    private Map<Integer, Integer> getContext(List<Integer> sentence) {
        Map<Integer, Integer> counts = new HashMap<>();
        for (int index : sentence) {
            if (counts.containsKey(index)) {
                counts.put(index, counts.get(index) + 1);
            } else {
                counts.put(index, 1);
            }
        }
        return counts;
    }

    public void generateMatrices() {
        System.out.println("Generating wordmap...");
        long startTime = System.nanoTime();
        WordmapGenerator wordmapGenerator
                = new WordmapGenerator(corpusRoot, tokenizedFileReaderFactory, numThreads, dim);
        wordMap = wordmapGenerator.generate();
        System.out.println(
                String.format("Wordmap generation took %d seconds",
                        (System.nanoTime() - startTime) / 1000000000));
        // Generate matrices.
        System.out.println("Generating matrices...");
        startTime = System.nanoTime();
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
        densityMatrices[min][max - min].updateEntry(word, diff);
    }

    private void updateVector(String word, int x, float diff) {
        vectors[x].updateEntry(word, diff);
    }

    public float[][] getMatrix(String target) {
        float[][] output = new float[dim][dim];
        for (int i = 0; i < dim; i++) {
            for (int j = i; j < dim; j++) {
                Float val = densityMatrices[i][j - i].getValue(target);
                if (val == null) {
                    output[i][j] = 0.0f;
                    output[j][i] = 0.0f;
                } else {
                    output[i][j] = val;
                    output[j][i] = val;
                }
            }
        }
        return output;
    }

    public void writeMatrices(String outputPath) {
        long startTime = System.nanoTime();
        Map<String, SparseDMatrixWriter> outputWriters = new HashMap<>();
        for (String target : targets) {
            outputWriters.put(target, new SparseDMatrixWriter(target, outputPath));
        }
        for (int x = 0; x < dim; x++) {
            for (int y = x; y < dim; y++) {
                for (Map.Entry<String, Float> entry : densityMatrices[x][y - x].getAllEntries()) {
                    outputWriters.get(entry.getKey()).writeEntry(x, y, entry.getValue());
                }
            }
        }
        // Close matrix writers.
        for (String target : targets) {
            outputWriters.get(target).close();
        }
        // Write matrix parameters.
        try {
            PrintWriter writer = new PrintWriter(Paths.get(outputPath, "parameters.txt").toString());
            writer.println(String.format("%s %d", "dimension", dim));
            writer.flush();
            writer.close();
        } catch (FileNotFoundException e) {
            System.out.println(String.format("File %s could not be created", outputPath));
        }
        System.out.println(String.format("Matrix write took %d seconds",
                (System.nanoTime() - startTime) / 1000000000));
    }

    public void writeVectors(String outputPath) {
        if (!getVectors) {
            System.out.println("Unable to write vectors, no vectors constructed.");
            return;
        }
        Map<String, Float[]> output = new HashMap<>();
        for (String target : targets) {
            output.put(target, new Float[dim]);
        }
        for (int i = 0; i < dim; i++) {
            for (String target : targets) {
                Float tmp = vectors[i].getValue(target);
                if (tmp == null) {
                    output.get(target)[i] = 0.0f;
                } else {
                    output.get(target)[i] = vectors[i].getValue(target);
                }
            }
        }
        try {
            PrintWriter writer = new PrintWriter(
                    new FileOutputStream(Paths.get(outputPath, "vectors.txt").toString()), false);
            for (Map.Entry<String, Float[]> entry : output.entrySet()) {
                StringBuilder stringBuilder = new StringBuilder();
                for (float value : entry.getValue()) {
                    stringBuilder.append(" ");
                    stringBuilder.append(value);
                }
                writer.println(String.format("%s%s", entry.getKey(), stringBuilder.toString()));
            }
            writer.flush();
            writer.close();
        } catch (FileNotFoundException e) {
            System.out.println(String.format("File %s could not be created", outputPath));
        }
    }

    public void writeWordmap(String outputPath) {
        try {
            PrintWriter writer = new PrintWriter(
                    new FileOutputStream(Paths.get(outputPath, "wordmap.txt").toString()), false);
            List<String> sorted = wordMap.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue()).map(Map.Entry::getKey).collect(Collectors.toList());
            sorted.forEach(writer::println);
            writer.flush();
            writer.close();
        } catch (FileNotFoundException e) {
            System.out.println("Unable to write wordmap.");
        }
    }

    private class DMatrixFileWorker implements Runnable {
        private List<String> filePaths;
        private DistributionalDMatrixGenerator dMatrixGenerator;

        DMatrixFileWorker(List<String> paths, DistributionalDMatrixGenerator dMatrixGenerator) {
            this.filePaths = paths;
            this.dMatrixGenerator = dMatrixGenerator;
        }

        public void run() {
            filePaths.forEach(this::processFile);
        }

        void processFile(String path) {
            TokenizedFileReader reader = tokenizedFileReaderFactory.getReader(path);
            String[] strTokens;
            while ((strTokens = reader.readLineTokens()) != null) {
                List<Integer> tokens = dMatrixGenerator.strTokensToIndices(strTokens);
                if (tokens.size() == 0) {
                    continue;
                }
                Map<Integer, Integer> context = dMatrixGenerator.getContext(tokens);
                float norm = (float) context.values().stream().mapToDouble(x -> x * x).sum();
                //TODO: The order of data structure usage is probably not optimal here.
                List<Integer[]> countList = context.entrySet().stream()
                        .map(entry -> new Integer[]{entry.getKey(), entry.getValue()})
                        .collect(Collectors.toList());
                List<Integer[]> pairs = new ArrayList<>();
                int index = 0;
                for (Integer[] current : countList) {
                    ListIterator<Integer[]> innerIter = countList.listIterator(index);
                    while (innerIter.hasNext()) {
                        Integer[] tmp = innerIter.next();
                        pairs.add(new Integer[]{current[0], current[1], tmp[0], tmp[1]});
                    }
                    index++;
                }
                for (String target : strTokens) {
                    if (dMatrixGenerator.targets.contains(target)) {
                        if (dMatrixGenerator.wordMap.containsKey(target)) {
                            int targetIndex = dMatrixGenerator.wordMap.get(target);
                            float newNorm = norm - 2 * context.get(targetIndex) + 1;
                            for (Integer[] data : pairs ) {
                                int xCount = data[0].equals(targetIndex) ? (data[1] - 1) : data[1];
                                int yCount = data[2].equals(targetIndex) ? (data[3] - 1) : data[3];
                                dMatrixGenerator.updateMatrix(target, data[0], data[2], xCount * yCount / newNorm);
                                if (getVectors && data[0].equals(data[2])) {
                                    dMatrixGenerator.updateVector(target, data[0], xCount / newNorm);
                                }
                            }
                        } else {
                            for (Integer[] data : pairs) {
                                dMatrixGenerator.updateMatrix(target, data[0], data[2], data[1] * data[3] / norm);
                                if (getVectors && data[0].equals(data[2])) {
                                    dMatrixGenerator.updateVector(target, data[0], data[1] / norm);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private class DataCell {
        Map<String, Float> entries;

        DataCell() {
            this.entries = new HashMap<>();
        }

        Set<Map.Entry<String, Float>> getAllEntries() {
            return entries.entrySet();
        }

        Float getValue(String target) {
            return entries.get(target);
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

}
