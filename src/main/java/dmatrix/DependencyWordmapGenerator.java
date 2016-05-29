package dmatrix;

import dmatrix.io.*;

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
public class DependencyWordmapGenerator {
    private String corpusRoot;
    private int numThreads;
    private Set<String> targets;

    public DependencyWordmapGenerator(String corpusRoot, Set<String> targets, int numThreads) {
        this.corpusRoot = corpusRoot;
        this.numThreads = numThreads;
        this.targets = targets;
    }

    public Map<String, Integer> generate() {
        List<String> mostFrequent = getMostFrequent();
        Map<String, Integer> output = new HashMap<>(mostFrequent.size());
        ListIterator<String> li = mostFrequent.listIterator(mostFrequent.size());
        int count = 0;
        while (li.hasPrevious()) {
            output.put(li.previous(), count);
            count++;
        }
        return output;
    }

    private List<String> getMostFrequent() {
        Map<String, Integer> counts = new HashMap<>();
        ExecutorService pool = Executors.newFixedThreadPool(this.numThreads);
        List<List<String>> filePathPartitions = IOUtils.getFilePathPartitions(corpusRoot, numThreads);
        for (List<String> filePathPartition : filePathPartitions) {
            pool.submit(new CountWorker(filePathPartition, counts));
        }
        pool.shutdown();
        try {
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            //TODO: see if graceful exit is possible here
        }
        List<Map.Entry<String,Integer>> tmp = counts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue()).collect(Collectors.toList());
        System.out.println(tmp);
        return tmp.stream().map(Map.Entry::getKey).collect(Collectors.toList());
    }

    private class CountWorker implements Runnable {
        private List<String> paths;
        private final Map<String, Integer> totalCounts;
        private SentenceStreamFactory sentenceStreamFactory;

        CountWorker(List<String> paths, Map<String, Integer> totalCounts) {
            this.paths = paths;
            this.totalCounts = totalCounts;
            sentenceStreamFactory = new SentenceStreamFactory(targets);
        }

        public void run() {
            for (String path : this.paths) {
                Map<String, Integer> counts = new HashMap<>();
                SentenceStream sentenceStream = sentenceStreamFactory.getStream(path);
                Sentence sentence;
                while ((sentence = sentenceStream.getSentence()) != null) {
                    for (String token : sentence.getWords()) {
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
