package dmatrix;

import dmatrix.DMatrixProtos.DMatrixSparse;
import dmatrix.io.IOUtils;
import dmatrix.io.TextFileReader;
import dmatrix.io.TokenizedFileReader;
import dmatrix.io.TokenizedFileReaderFactory;

import java.io.*;
import java.lang.Runnable;
import java.lang.InterruptedException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Density matrix generator using sparse updates.
 * <p>
 * Created by zhuoranzhang on 4/16/16.
 */
public class DistributionalDMatrixGenerator {

    private static final float density = 0.22f;

    // Runtime parameters.
    private String corpusRoot;
    private int numThreads;
    private int dim;
    private boolean getVectors;
    private Set<String> targets;
    private TokenizedFileReaderFactory tokenizedFileReaderFactory;

    private Map<String, Integer> wordMap;
    private Map<String, Map<Pair<Integer,Integer>, Float>> densityMatrices;
    private Map<String, Float[]> vectors;

    public static void main(String[] args) {
        String corpusRoot = args[0];
        String targetsPath = args[1];
        int dim = Integer.parseInt(args[2]);
        int numThreads = Integer.parseInt(args[3]);
        boolean getVectors = (Integer.parseInt(args[5]) == 1);
        boolean writeWordmap = (Integer.parseInt(args[6]) == 1);
        DistributionalDMatrixGenerator dmg = new DistributionalDMatrixGenerator(corpusRoot, targetsPath, dim, numThreads, getVectors);
        dmg.generateMatrices();
        dmg.writeMatrices(args[4]);
        if (getVectors) {
            dmg.writeVectors(args[4]);
        }
        if (writeWordmap) {
            dmg.writeWordmap(args[4]);
        }
    }

    public DistributionalDMatrixGenerator(String corpusRoot, String targetsPath, int dim, int numThreads, boolean getVectors) {
        this.corpusRoot = corpusRoot;
        this.numThreads = numThreads;
        this.dim = dim;
        this.getVectors = getVectors;
        this.tokenizedFileReaderFactory = new TokenizedFileReaderFactory();
        this.loadTargets(targetsPath);
        this.densityMatrices = new HashMap<>();
        this.vectors = new HashMap<>();
        for (String target : targets) {
            densityMatrices.put(target, new HashMap<>((int) Math.ceil(density * dim * dim / 2)));
            vectors.put(target, new Float[dim]);
        }
    }

    private void loadTargets(String targetsPath) {
        this.targets = new HashSet<>();
        TextFileReader reader = new TextFileReader(targetsPath);
        String line;
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\\s+");
            for (String s : tmp) {
                this.targets.add(s.toLowerCase());
            }
        }
    }

    private void generateWordmap(List<String> filePaths) {
        Map<String, Integer> counts = new HashMap<>();
        for (String filePath : filePaths) {
            TokenizedFileReader reader = tokenizedFileReaderFactory.getReader(filePath);
            String[] tokens;
            while ((tokens = reader.readLineTokens()) != null) {
                for (String token : tokens) {
                    if (counts.containsKey(token)) {
                        counts.put(token, counts.get(token) + 1);
                    } else {
                        counts.put(token, 1);
                    }
                }
            }
        }
        List<Map.Entry<String, Integer>> sorted = counts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue()).collect(Collectors.toList());
        ListIterator<Map.Entry<String, Integer>> li = sorted.listIterator(sorted.size());
        int index = 0;
        Map<String, Integer> wordMap = new HashMap<>();
        while (index < this.dim && li.hasPrevious()) {
            wordMap.put(li.previous().getKey(), index);
            index++;
        }
        this.wordMap = wordMap;
    }

    private List<Integer> strTokensToIndices(String[] strTokens) {
        List<Integer> output = new ArrayList<>();
        for (String strToken : strTokens) {
            if (this.wordMap.containsKey(strToken)) {
                output.add(this.wordMap.get(strToken));
            }
        }
        return output;
    }

    protected List<Integer[]> getContext(List<Integer> sentence) {
        Map<Integer, Integer> counts = new HashMap<>();
        for (int index : sentence) {
            if (counts.containsKey(index)) {
                counts.put(index, counts.get(index) + 1);
            } else {
                counts.put(index, 1);
            }
        }
        return counts.entrySet().stream().map(entry -> new Integer[]{entry.getKey(), entry.getValue()})
                .collect(Collectors.toList());
    }

    public void generateMatrices() {
        List<String> filePaths = IOUtils.getFilePaths(corpusRoot);
        List<List<String>> filePathPartitions = IOUtils.getFilePathPartitions(corpusRoot, numThreads);
        System.out.println("Generating wordmap...");
        long startTime = System.nanoTime();
        generateWordmap(filePaths);
        System.out.println(
                String.format("Wordmap generation took %d seconds",
                        (System.nanoTime() - startTime) / 1000000000));
        System.out.println("Generating matrices...");
        startTime = System.nanoTime();
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

    private void updateMatrix(String target, List<Integer[]> context) {
        Map<Pair<Integer, Integer>, Float> matrix = densityMatrices.get(target);
        Integer targetInd = wordMap.get(target);
        synchronized (matrix) { // TODO: Unsafe, consider wrapping matrices.
            int index = 0;
            for (Integer[] current : context) {
                ListIterator<Integer[]> innerIter = context.listIterator(index);
                while (innerIter.hasNext()) {
                    Integer[] tmp = innerIter.next();
                    Pair<Integer, Integer> coords = new ImmutablePair(Math.min(current[0], tmp[0]), Math.max(current[0], tmp[0]));
                    Float prev = matrix.get(coords);
                    Float value;
                    if (current[0].equals(targetInd)) {
                        value = (float) current[1] - 1;
                    } else {
                        value = (float) current[1];
                    }
                    if (tmp[0].equals(targetInd)) {
                        value = value * (tmp[1] - 1);
                    } else {
                        value = value * tmp[1];
                    }
                    if (prev == null) {
                        matrix.put(coords, value);
                    } else {
                        matrix.put(coords, prev + value);
                    }
                }
                index++;
            }
        }
    }

    public float[][] getMatrix(String target) {
        float[][] output = new float[dim][dim];
        for (Map.Entry<Pair<Integer, Integer>, Float> entry : densityMatrices.get(target).entrySet()) {
            int x = entry.getKey().getLeft();
            int y = entry.getKey().getRight();
            output[x][y] = entry.getValue();
            output[y][x] = entry.getValue();
        }
        return output;
    }

    public void writeMatrices(String outputPath) {
        for (String target : targets) {
            DMatrixSparse.Builder targetMatrix = DMatrixSparse.newBuilder();
            targetMatrix.setWord(target);
            targetMatrix.setDimension(dim);
            for (Map.Entry<Pair<Integer, Integer>, Float> entry : densityMatrices.get(target).entrySet()) {
                DMatrixSparse.DMatrixEntry.Builder dMatrixEntry = DMatrixSparse.DMatrixEntry.newBuilder();
                dMatrixEntry.setX(entry.getKey().getLeft());
                dMatrixEntry.setY(entry.getKey().getRight());
                dMatrixEntry.setValue(entry.getValue());
                targetMatrix.addEntries(dMatrixEntry.build());
            }
            try {
                FileOutputStream outputStream = IOUtils.getOutputStream(outputPath, target);
                targetMatrix.build().writeTo(outputStream);
                outputStream.close();
            } catch (IOException e) {
                System.out.println("Failed to write matrices to output.");
            }
        }
    }

    public void writeVectors(String outputPath) {
        if (!getVectors) {
            System.out.println("Unable to write vectors, no vectors constructed.");
            return;
        }
        try {
            PrintWriter writer = new PrintWriter(
                    new FileOutputStream(Paths.get(outputPath, "vectors.txt").toString()), false);
            for (Map.Entry<String, Float[]> entry : vectors.entrySet()) {
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
            for (String word : sorted) {
                writer.println(word);
            }
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
            for (String path : filePaths) {
                processFile(path);
            }
        }

        void processFile(String path) {
            TokenizedFileReader reader = tokenizedFileReaderFactory.getReader(path);
            String[] strTokens;
            while ((strTokens = reader.readLineTokens()) != null) {
                List<Integer> tokens = dMatrixGenerator.strTokensToIndices(strTokens);
                if (tokens.size() == 0) {
                    continue;
                }
                List<Integer[]> context = dMatrixGenerator.getContext(tokens);
                for (String target : strTokens) {
                    if (dMatrixGenerator.targets.contains(target)) {
                        dMatrixGenerator.updateMatrix(target, context);
                    }
                }
            }
        }
    }

}
