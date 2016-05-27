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
    private String corpus;
    private int numThreads;
    private Set<String> targets;

    private final Map<String, Integer> wordMap;
    private int wordMapIndex;
    private Map<String, Map<Pair, Float>> densityMatrices;

    public static void main(String[] args) throws IOException {
        String corpusRoot = args[0];
        String targetsPath = args[1];
        int numThreads = Integer.parseInt(args[2]);
        DependencyDMatrixGenerator dmg = new DependencyDMatrixGenerator(corpusRoot, targetsPath, numThreads);
        dmg.generateMatrices();
        dmg.writeMatrices(args[3]);
        dmg.writeWordmap(args[3]);
    }

    public DependencyDMatrixGenerator(String corpus, String targetsPath, int numThreads) {
        this.corpus = corpus;
        this.numThreads = numThreads;
        this.loadTargets(targetsPath);
        this.wordMap = new HashMap<>();
        wordMapIndex = 0;
        densityMatrices = new HashMap<>();
        for (String target : targets) {
            densityMatrices.put(target, new HashMap<>());
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
        SentenceStream sentenceStream = SentenceStream.getStream(corpus);
        ExecutorService pool = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++) {
            System.out.println(i);
            pool.submit(new DMatrixSentenceWorker(sentenceStream, this));
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
        for (String word : context.keySet()) {
            if (!wordMap.containsKey(word)) {
                synchronized(wordMap) {
                    if (!wordMap.containsKey(word)) {
                        wordMap.put(word, wordMapIndex);
                        wordMapIndex += 1;
                    }
                }

            }
        }
        List<Pair<Integer, Integer>> intContext = context.entrySet().stream().map(
                entry -> new ImmutablePair<>(wordMap.get(entry.getKey()), entry.getValue()))
                .sorted().collect(Collectors.toList());
        int index = 0;
        for (Pair<Integer, Integer> outer : intContext) {
            for (Pair<Integer, Integer> inner : intContext.subList(index, intContext.size())) {
                Pair<Integer, Integer> coords = new ImmutablePair<>(outer.getLeft(), inner.getLeft());
                Map<Pair, Float> targetMatrix = densityMatrices.get(target);
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
    }

    public void writeMatrices(String outputPath) {
        long startTime = System.nanoTime();
        for (String target : targets) {
            Map<Pair, Float> matrix = densityMatrices.get(target);
            if (matrix.size() == 0) {
                continue;
            }
            SparseDMatrixWriter writer = new SparseDMatrixWriter(target, outputPath);
            for (Map.Entry<Pair, Float> entry : matrix.entrySet()) {
                Pair<Integer, Integer> coord = entry.getKey();
                writer.writeEntry(coord.getLeft(), coord.getRight(), entry.getValue());
            }
            writer.close();
        }
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
        private SentenceStream sentenceStream;
        private DependencyDMatrixGenerator dMatrixGenerator;

        DMatrixSentenceWorker(SentenceStream sentenceStream, DependencyDMatrixGenerator dMatrixGenerator) {
            this.sentenceStream = sentenceStream;
            this.dMatrixGenerator = dMatrixGenerator;
        }

        public void run() {
            Sentence sentence;
            while ((sentence = sentenceStream.getSentence()) != null) {
                processSentence(sentence);
            }
        }

        private void processSentence(Sentence sentence) {
            Map<Integer, Map<String, Integer>> relationCounts = new HashMap<>(sentence.size());
            for (int[] dep : sentence.getDependencies()) {
                for (int i = 0; i < 2; i++) {
                    String word1 = sentence.getWord(dep[i]);
                    String word2 = sentence.getWord(dep[(i + 1) % 2]);
                    if (dMatrixGenerator.targets.contains(word1)) {
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
                dMatrixGenerator.updateMatrix(sentence.getWord(entry.getKey()), entry.getValue());
            }
        }

    }

}
