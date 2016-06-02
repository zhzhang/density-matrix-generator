package dmatrix;

import dmatrix.io.IOUtils;
import dmatrix.io.TextFileReader;
import dmatrix.io.TokenizedFileReader;
import dmatrix.io.TokenizedFileReaderFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Maps context words to specific values.
 * <p>
 * Created by zhuoranzhang on 5/12/16.
 */
public class WordmapGenerator {
    private String corpusRoot;
    private TokenizedFileReaderFactory tokenizedFileReaderFactory;
    private int numThreads;
    private int numContexts;
    private int cutoff;

    public WordmapGenerator(String corpusRoot, TokenizedFileReaderFactory tokenizedFileReaderFactory, int numThreads, int numContexts) {
        this.corpusRoot = corpusRoot;
        this.tokenizedFileReaderFactory = tokenizedFileReaderFactory;
        this.numThreads = numThreads;
        this.numContexts = numContexts;
        cutoff = 0;
    }

    public Map<String, Integer> generate() {
        List<String> topN = getMostFrequent();
        if (numContexts == 0) {
            numContexts = topN.size();
        }
        Map<String, Integer> output = new HashMap<>(numContexts);
        int count = 0;
        for (String word : topN) {
            output.put(word, count);
            count++;
        }
        return output;
    }

    public int getCutoff() {
        return cutoff;
    }

    public Map<String, float[]> generate(String vectorsPath) {
        Set<String> topN = new HashSet<>(getMostFrequent());
        TextFileReader reader = new TextFileReader(vectorsPath);
        Map<String, float[]> wordMap = new HashMap<>();
        String line;
        while ((line = reader.readLine()) != null) {
            String[] tokens = line.split("\\s+");
            if (topN.contains(tokens[0])) {
                float[] vector = new float[tokens.length - 1];
                for (int i = 1; i < tokens.length; i++) {
                    vector[i - 1] = Float.parseFloat(tokens[i]);
                }
                wordMap.put(tokens[0], vector);
            }
        }
        if (wordMap.size() < numContexts) {
            System.out.println(String.format(
                    "WARNING: vectors exist for only %d out of %d context words", wordMap.size(), numContexts));
        }
        return wordMap;
    }

    private List<String> getMostFrequent() {
        Map<String, Integer> counts = new HashMap<>();
        ExecutorService pool = Executors.newFixedThreadPool(this.numThreads);
        List<List<String>> filePathPartitions = IOUtils.getFilePathPartitions(corpusRoot, numThreads);
        for (List<String> filePathPartition : filePathPartitions) {
            pool.submit(new CountWorker(filePathPartition, tokenizedFileReaderFactory, counts));
        }
        pool.shutdown();
        try {
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            //TODO: see if graceful exit is possible here
        }
        List<Map.Entry<String, Integer>> sorted = counts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue()).collect(Collectors.toList());
        Collections.reverse(sorted);
        if (numContexts <= 0 ) {
            int total = sorted.stream().mapToInt(Map.Entry::getValue).sum();
            int sum = 0;
            cutoff = 0;
            for (Map.Entry<String, Integer> entry : sorted) {
                sum += entry.getValue();
                cutoff++;
                if ((float) sum / total > 0.85) {
                    break;
                }
            }
            List<String> output = sorted.stream().map(Map.Entry::getKey).collect(Collectors.toList());
            return output;
        }
        List<String> output = new ArrayList<>(numContexts);
        int count = 0;
        for (Map.Entry<String, Integer> entry : sorted) {
            output.add(entry.getKey());
            count++;
            if (count == numContexts) {
                break;
            }
        }
        return output;
    }

    private class CountWorker implements Runnable {
        private List<String> paths;
        private TokenizedFileReaderFactory tokenizedFileReaderFactory;
        private final Map<String, Integer> totalCounts;

        CountWorker(List<String> paths, TokenizedFileReaderFactory tokenizedFileReaderFactory, Map<String, Integer> totalCounts) {
            this.paths = paths;
            this.tokenizedFileReaderFactory = tokenizedFileReaderFactory;
            this.totalCounts = totalCounts;
        }

        public void run() {
            for (String path : this.paths) {
                Map<String, Integer> counts = new HashMap<>();
                TokenizedFileReader reader = tokenizedFileReaderFactory.getReader(path);
                String[] tokens;
                while ((tokens = reader.readLineTokens()) != null) {
                    for (String token : tokens) {
                        Integer prev = counts.get(token);
                        if (prev == null) {
                            counts.put(token, 1);
                        } else {
                            counts.put(token, prev + 1);
                        }
                    }
                }
                this.updateCounts(counts);
            }
        }

        private void updateCounts(Map<String, Integer> diff) {
            synchronized (totalCounts) {
                for (Map.Entry<String, Integer> entry : diff.entrySet()) {
                    String target = entry.getKey();
                    Integer prev = totalCounts.get(target);
                    if (prev == null) {
                        totalCounts.put(target, entry.getValue());
                    } else {
                        totalCounts.put(target, prev + entry.getValue());
                    }
                }
            }
        }
    }
}
