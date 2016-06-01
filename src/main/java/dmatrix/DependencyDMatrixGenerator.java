package dmatrix;

import dmatrix.io.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Density matrix generator using sparse updates.
 * <p>
 * Created by zhuoranzhang on 4/16/16.
 */
public class DependencyDMatrixGenerator {

    // Runtime parameters.
    private String corpusRoot;
    private int numThreads;
    private Set<String> targets;

    private final Map<String, Integer> wordMap;
    private DataCell[][] densityMatrices;
    private Map<String, Map<Pair<Integer, Integer>, Float>> densityMatricesSparse;
    private int cutoff;
    private boolean softCutoff;

    public static void main(String[] args) throws IOException {
        String corpusRoot = args[0];
        String targetsPath = args[1];
        int dim = Integer.parseInt(args[2]);
        int numThreads = Integer.parseInt(args[3]);
        DependencyDMatrixGenerator dmg = new DependencyDMatrixGenerator(corpusRoot, targetsPath, dim, numThreads);
        dmg.generateMatrices();
        dmg.writeMatrices(args[4]);
        dmg.writeWordmap(args[4]);
    }

    public DependencyDMatrixGenerator(String corpusRoot, String targetsPath, int dim, int numThreads) {
        this.corpusRoot = corpusRoot;
        this.numThreads = numThreads;
        this.loadTargets(targetsPath);
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
        densityMatricesSparse = new HashMap<>();
        for (String target : targets) {
            densityMatricesSparse.put(target, new HashMap<>());
        }
        densityMatrices = new DataCell[cutoff][];
        for (int i = 0; i < cutoff; i++) {
            densityMatrices[i] = new DataCell[cutoff - i];
            for (int j = 0; j < cutoff - i; j++) {
                densityMatrices[i][j] = new DataCell();
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

    public void generateMatrices() throws IOException {
        // Generate matrices.
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

    private void updateMatrix(String target, Map<String, Integer> context) {
        List<Pair<Integer, Integer>> intContext = context.entrySet().stream().map(
                entry -> new ImmutablePair<>(wordMap.get(entry.getKey()), entry.getValue()))
                .sorted().collect(Collectors.toList());
        int index = 0;
        for (Pair<Integer, Integer> outer : intContext) {
            for (Pair<Integer, Integer> inner : intContext.subList(index, intContext.size())) {
                int x = outer.getLeft();
                int y = inner.getLeft();
                if (x < cutoff && y < cutoff) {
                    densityMatrices[x][y - x]
                            .updateEntry(target, outer.getRight() * inner.getRight());
                } else if (softCutoff) {
                    Pair<Integer, Integer> coords = new ImmutablePair<>(x, y);
                    Map<Pair<Integer, Integer>, Float> targetMatrix = densityMatricesSparse.get(target);
                    synchronized (targetMatrix) {
                        Float prev = targetMatrix.get(coords);
                        if (prev == null) {
                            targetMatrix.put(coords, (float) outer.getRight() * inner.getRight());
                        } else {
                            targetMatrix.put(coords, prev + (float) outer.getRight() * inner.getRight());
                        }
                    }
                }
            }
            index++;
        }
    }

    public float[][] getMatrix(String target) {
        float[][] output;
        if (softCutoff) {
            output = new float[wordMap.size()][wordMap.size()];
        } else {
            output = new float[cutoff][cutoff];
        }
        for (int i = 0; i < cutoff; i++) {
            for (int j = i; j < cutoff; j++) {
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
        if (softCutoff) {
            Map<Pair<Integer, Integer>, Float> targetMatrix = densityMatricesSparse.get(target);
            for (Map.Entry<Pair<Integer, Integer>, Float>  entry : targetMatrix.entrySet()) {
                int x = entry.getKey().getLeft();
                int y = entry.getKey().getRight();
                output[x][y] = entry.getValue();
                output[y][x] = entry.getValue();
            }
        }
        return output;
    }

    public void writeMatrices(String outputPath) {
        long startTime = System.nanoTime();
        Map<String, SparseDMatrixWriter> outputWriters = new HashMap<>();
        for (int x = 0; x < cutoff; x++) {
            for (int y = x; y < cutoff; y++) {
                for (Map.Entry<String, Float> entry : densityMatrices[x][y - x].getAllEntries()) {
                    SparseDMatrixWriter writer = outputWriters.get(entry.getKey());
                    if (writer == null) {
                        writer = new SparseDMatrixWriter(entry.getKey(), outputPath);
                        outputWriters.put(entry.getKey(), writer);
                    }
                    writer.writeEntry(x, y, entry.getValue());
                }
            }
        }
        for (String target : outputWriters.keySet()) {
            Map<Pair<Integer, Integer>, Float> matrix = densityMatricesSparse.get(target);
            if (matrix.size() == 0) {
                continue;
            }
            SparseDMatrixWriter writer = outputWriters.get(target);
            for (Map.Entry<Pair<Integer, Integer>, Float> entry : matrix.entrySet()) {
                Pair<Integer, Integer> coord = entry.getKey();
                writer.writeEntry(coord.getLeft(), coord.getRight(), entry.getValue());
            }
        }
        // Close matrix writers.
        outputWriters.values().forEach(SparseDMatrixWriter::close);
        // Write matrix parameters.
        try {
            PrintWriter writer = new PrintWriter(Paths.get(outputPath, "parameters.txt").toString());
            writer.println(String.format("%s %d", "dimension", wordMap.size()));
            writer.flush();
            writer.close();
        } catch (FileNotFoundException e) {
            System.out.println(String.format("File %s could not be created", outputPath));
        }
        System.out.println(String.format("Matrix write took %d seconds",
                (System.nanoTime() - startTime) / 1000000000));
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

        synchronized void updateEntry(String target, float diff) {
            Float prev = entries.get(target);
            if (prev == null) {
                entries.put(target, diff);
            } else {
                entries.put(target, prev + diff);
            }
        }
    }

}
