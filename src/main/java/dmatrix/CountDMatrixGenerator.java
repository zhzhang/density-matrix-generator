package dmatrix;

import dmatrix.io.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Density matrix generator using sparse updates.
 * <p>
 * Created by zhuoranzhang on 4/16/16.
 */
public abstract class CountDMatrixGenerator {

    // Runtime parameters.
    String corpusRoot;
    int numThreads;
    int cutoff;
    boolean getVectors;
    boolean softCutoff;
    Set<String> targets;

    Map<String, Integer> wordMap;
    DataCell[][] densityMatrices;
    private Map<String, Map<Pair<Integer, Integer>, Float>> densityMatricesSparse;
    DataCell[] vectors;

    CountDMatrixGenerator(String corpusRoot, String targetsPath, int dim, int numThreads, boolean getVectors) {
        this.corpusRoot = corpusRoot;
        this.numThreads = numThreads;
        this.loadTargets(targetsPath);
        generateWordmap(dim);
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
        if (getVectors) {
            vectors = new DataCell[cutoff];
            for (int i = 0; i < cutoff; i++) {
                vectors[i] = new DataCell();
            }

        }
    }

    abstract void generateWordmap(int dim);

    public abstract void generateMatrices() throws IOException;

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

    void updateMatrix(String target, Map<String, Integer> context) {
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

    private void updateVector(String word, int x, float diff) {
        vectors[x].updateEntry(word, diff);
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

    public void writeVectors(String outputPath) {
        if (!getVectors) {
            System.out.println("Unable to write vectors, no vectors constructed.");
            return;
        }
        Map<String, Float[]> output = new HashMap<>();
        for (String target : targets) {
            output.put(target, new Float[cutoff]);
        }
        for (int i = 0; i < cutoff; i++) {
            for (Map.Entry<String, Float> entry : vectors[i].getAllEntries()) {
                Float[] vector = output.get(entry.getKey());
                if (vector == null) {
                    vector = new Float[cutoff];
                    output.put(entry.getKey(), vector);
                }
                vector[i] = entry.getValue();
            }
        }
        try {
            PrintWriter writer = new PrintWriter(
                    new FileOutputStream(Paths.get(outputPath, "vectors.txt").toString()), false);
            for (Map.Entry<String, Float[]> entry : output.entrySet()) {
                StringBuilder stringBuilder = new StringBuilder();
                for (Float value : entry.getValue()) {
                    stringBuilder.append(" ");
                    if (value == null) {
                        stringBuilder.append(0);
                    } else {
                        stringBuilder.append(value);
                    }
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

    class DataCell {
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