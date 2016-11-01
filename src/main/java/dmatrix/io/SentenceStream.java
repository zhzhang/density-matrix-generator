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

    private String path;
    private MessageUnpacker unpacker;
    private Set<String> targets;
    private static Set<String> stopList = new HashSet<>(Arrays.asList(
            new String[]{"det", "case", "punct", "mark", "cc", "root", "dep", "expl", "cop",
                    "aux", "auxpass", "discourse", "vocative"}));

    public SentenceStream(String path, Set<String> targets) throws IOException {
        this.path = path;
        this.targets = targets;
        unpacker = MessagePack.newDefaultUnpacker(new GZIPInputStream(new FileInputStream(path)));
    }

    public synchronized Sentence getSentence() {
        try {
            if (!unpacker.hasNext()) {
                return null;
            }
            int numWords = unpacker.unpackArrayHeader();
            String[] words = new String[numWords];
            for (int i = 0; i < numWords; i++) {
                words[i] = unpacker.unpackString().toLowerCase();
            }
            int numCollapsedDeps = unpacker.unpackArrayHeader();
            List<Integer[]> dependencies = new ArrayList<>(numCollapsedDeps - 1);
            Map<Integer, String> wordMap = new HashMap<>();
            for (int i = 0; i < numCollapsedDeps; i++) {
                String relType = unpacker.unpackString().split(":")[0].toLowerCase();
                Integer[] dep = new Integer[2];
                dep[0] = unpacker.unpackInt() - 1;
                dep[1] = unpacker.unpackInt() - 1;
                if (dep[0] < 0 || dep[1] < 0 || stopList.contains(relType)) {
                    continue;
                }
                if (targets.contains(words[dep[0]]) || targets.contains(words[dep[1]])) {
                    dependencies.add(dep);
                    wordMap.put(dep[0], words[dep[0]]);
                    wordMap.put(dep[1], words[dep[1]]);
                }
            }
            return new Sentence(wordMap, dependencies);
        } catch (IOException e) {
            System.out.println(String.format("Got error \"%s\" for input file %s", e.getMessage(), path));
            return null;
        }
    }

    public String getPath() {
        return path;
    }

    public void close() {
        try {
            unpacker.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

