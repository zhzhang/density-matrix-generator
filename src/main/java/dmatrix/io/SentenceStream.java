package dmatrix.io;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * MessageUnpacker wrapper for parsed wikipedia corpus.
 * <p>
 * Created by zhuoranzhang on 5/27/16.
 */
public class SentenceStream {

    private MessageUnpacker unpacker;
    private Set<String> targets;
    private int numSentences;
    private int numRead;
    private static Set<String> stopList = new HashSet<>(Arrays.asList(
            new String[]{"det", "punct", "mark", "cc", "case", "cop", "root", "dep"}));

    public SentenceStream(String path, Set<String> targets) throws IOException {
        this.targets = targets;
        unpacker = MessagePack.newDefaultUnpacker(new GZIPInputStream(new FileInputStream(path)));
        numSentences = (unpacker.unpackArrayHeader());
        numRead = 0;
    }

    public synchronized Sentence getSentence() {
        if (numRead == numSentences) {
            return null;
        }
        numRead++;
        //System.out.println(String.format("Processing sentence %d", numRead));
        try {
            unpacker.unpackArrayHeader(); // Unpack the sentence array header.
            int numWords = unpacker.unpackArrayHeader();
            String[] words = new String[numWords];
            for (int i = 0; i < numWords; i++) {
                words[i] = unpacker.unpackString().toLowerCase();
            }
            int numCollapsedDeps = unpacker.unpackArrayHeader();
            List<Integer[]> dependencies = new ArrayList<>(numCollapsedDeps - 1);
            // Skip the first dependency, assuming the ROOT dependency is always first.
            unpacker.unpackArrayHeader();
            unpacker.unpackString();
            unpacker.unpackInt();
            unpacker.unpackInt();
            Map<Integer, String> wordMap = new HashMap<>();
            for (int i = 0; i < numCollapsedDeps - 1; i++) {
                unpacker.unpackArrayHeader();
                String relType = unpacker.unpackString();
                Integer[] dep = new Integer[2];
                dep[0] = unpacker.unpackInt() - 1;
                dep[1] = unpacker.unpackInt() - 1;
                if (stopList.contains(relType)) {
                    continue;
                }
                /*
                if (words[dep[0]].equals("``") || words[dep[1]].equals("``")) {
                    System.out.println(relType);
                }*/
                if (targets.contains(words[dep[0]]) || targets.contains(words[dep[1]])) {
                    dependencies.add(dep);
                    wordMap.put(dep[0], words[dep[0]]);
                    wordMap.put(dep[1], words[dep[1]]);
                }
            }
            return new Sentence(wordMap, dependencies);
        } catch (IOException e) {
            return null;
        }
    }

}

