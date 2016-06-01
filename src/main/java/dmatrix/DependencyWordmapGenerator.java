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
    private int cutoff;

    public DependencyWordmapGenerator(String corpusRoot, Set<String> targets, int numThreads) {
        this.corpusRoot = corpusRoot;
        this.numThreads = numThreads;
        this.targets = targets;
        cutoff = 0;
    }

    public Map<String, Integer> generate() {
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
        Map<String, Integer> output = new HashMap<>(counts.size());
        ListIterator<Map.Entry<String, Integer>> li = counts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue()).collect(Collectors.toList()).listIterator(counts.size());
        int index = 0;
        int total = counts.values().stream().mapToInt(Integer::intValue).sum();
        int partialCount = 0;
        /*
        boolean ninety = true;
        boolean ninetyfive = true;*/
        while (li.hasPrevious()) {
            Map.Entry<String, Integer> entry = li.previous();
            partialCount += entry.getValue();
            if (cutoff == 0 && (float) partialCount / total > 0.85) {
                cutoff = index + 1;
            }/*
            if (ninety && (float) partialCount / total > 0.90) {
                System.out.println(String.format("90 percent at %d", index+1));
                ninety = false;
            } else if (ninetyfive  && (float) partialCount / total > 0.95) {
                System.out.println(String.format("95 percent at %d", index+1));
                ninetyfive = false;
            }*/
            output.put(entry.getKey(), index);
            index++;
        }
        return output;
    }

    public int getCutoff() {
        return cutoff;
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
