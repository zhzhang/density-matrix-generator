package dmatrix;

import dmatrix.DMatrixProtos.DMatrixSparse;
import dmatrix.io.IOUtils;
import dmatrix.io.TextFileReader;
import dmatrix.io.TokenizedFileReader;
import dmatrix.io.TokenizedFileReaderFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.Runnable;
import java.lang.InterruptedException;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ListIterator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DMatrixGenerator {

    // Runtime parameters.
    private String corpusRoot;
    private int numThreads;
    private int dim;
    protected Set<String> targets;
    private TokenizedFileReaderFactory tokenizedFileReaderFactory;

    protected Map<String, Integer> wordMap;
    private DMatrixCell[][] densityMatrices;

    public static void main(String[] args) {
        DMatrixGenerator dmg = new DMatrixGenerator(args[0], args[1],
                Integer.parseInt(args[2]), Integer.parseInt(args[3]));
        dmg.generateMatrices();
        dmg.writeMatrices(args[4]);
    }

    public DMatrixGenerator(String corpusRoot, String targetsPath, int dim, int numThreads) {
        this.corpusRoot = corpusRoot;
        this.numThreads = numThreads;
        this.dim = dim;
        this.tokenizedFileReaderFactory = new TokenizedFileReaderFactory();
        this.loadTargets(targetsPath);
        this.densityMatrices = new DMatrixCell[this.dim][];
        for (int i = 0; i < this.dim; i++) {
            this.densityMatrices[i] = new DMatrixCell[this.dim - i];
            for (int j = 0; j < this.dim - i; j++) {
                this.densityMatrices[i][j] = new DMatrixCell(i, j + i);
            }
        }
    }

    private void loadTargets(String targetsPath) {
        this.targets = new HashSet<>();
        TextFileReader reader = new TextFileReader(targetsPath);
        String line;
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\\s+");
            for (String s : tmp) {
                this.targets.add(s);
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

    protected List<Integer> strTokensToIndices(String[] strTokens) {
        List<Integer> output = new ArrayList<>();
        for (int i = 0; i < strTokens.length; i++) {
            if (this.wordMap.containsKey(strTokens[i])) {
                output.add(this.wordMap.get(strTokens[i]));
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
        List<Integer[]> countList = counts.entrySet().stream()
                .map(entry -> new Integer[]{entry.getKey(), entry.getValue()})
                .collect(Collectors.toList());
        List<Integer[]> output = new ArrayList<>();
        int index = 0;
        ListIterator<Integer[]> mainIter = countList.listIterator();
        while (mainIter.hasNext()) {
            Integer[] current = mainIter.next();
            ListIterator<Integer[]> innerIter = countList.listIterator(index);
            while (innerIter.hasNext()) {
                Integer[] tmp = innerIter.next();
                output.add(new Integer[]{current[0], current[1], tmp[0], tmp[1]});
            }
            index++;
        }
        return output;
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

    private void updateMatrix(String word, int x, int y, int diff) {
        int min;
        int max;
        if (x < y) {
            min = x;
            max = y;
        } else {
            min = y;
            max = x;
        }
        this.densityMatrices[min][max - min].updateEntry(word, diff);
    }

    public float[][] getMatrix(String target) {
        float[][] output = new float[dim][dim];
        for (int i = 0; i < dim; i++) {
            for (int j = i; j < dim; j++) {
                Float val = densityMatrices[i][j - i].getEntry(target);
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
        Map<String, DMatrixSparse.Builder> outputMatrices
                = new HashMap<String, DMatrixSparse.Builder>();
        for (String target : targets) {
            DMatrixSparse.Builder targetMatrix = DMatrixSparse.newBuilder();
            targetMatrix.setWord(target);
            targetMatrix.setDimension(dim);
            outputMatrices.put(target, targetMatrix);
        }
        for (DMatrixCell[] tmp : densityMatrices) {
            for (DMatrixCell cell : tmp) {
                for (Map.Entry<String, Float> entry : cell.getAllEntries()) {
                    DMatrixSparse.DMatrixEntry.Builder dMatrixEntry = DMatrixSparse.DMatrixEntry.newBuilder();
                    dMatrixEntry.setX(cell.x).setY(cell.y).setVal(entry.getValue());
                    outputMatrices.get(entry.getKey()).addEntries(dMatrixEntry.build());
                }
            }
        }
        for (String target : targets) {
            try {
                FileOutputStream outputStream = IOUtils.getOutputStream(outputPath, target);
                outputMatrices.get(target).build().writeTo(outputStream);
                outputStream.close();
            } catch (IOException e) {
                System.out.println("Failed to write matrices to output.");
            }
        }
    }

    class DMatrixFileWorker implements Runnable {
        private List<String> filePaths;
        private DMatrixGenerator dMatrixGenerator;

        DMatrixFileWorker(List<String> paths, DMatrixGenerator dMatrixGenerator) {
            this.filePaths = paths;
            this.dMatrixGenerator = dMatrixGenerator;
        }

        public void run() {
            for (String path : filePaths) {
                this.processFile(path);
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
                List<Integer[]> context = this.dMatrixGenerator.getContext(tokens);
                //TODO: The order of data structure usage is probably not optimal here.
                for (String target : strTokens) {
                    if (dMatrixGenerator.targets.contains(target)) {
                        if (dMatrixGenerator.wordMap.containsKey(target)) {
                            int targetIndex = dMatrixGenerator.wordMap.get(target);
                            for (Integer[] data : context) {
                                int xCount = data[0].equals(targetIndex) ? (data[1] - 1) : data[1];
                                int yCount = data[2].equals(targetIndex) ? (data[3] - 1) : data[3];
                                dMatrixGenerator.updateMatrix(target, data[0], data[2], xCount * yCount);
                            }
                        } else {
                            for (Integer[] data : context) {
                                dMatrixGenerator.updateMatrix(target, data[0], data[2], data[1] * data[3]);
                            }
                        }
                    }
                }
            }
        }
    }

    private class DMatrixCell {
        int x;
        int y;
        Map<String, Float> entries;

        DMatrixCell(int x, int y) {
            this.x = x;
            this.y = y;
            this.entries = new HashMap<>();
        }

        Set<Map.Entry<String, Float>> getAllEntries() {
            return entries.entrySet();
        }

        float getEntry(String target) {
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
