package dmatrix.io;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

/**
 * MessageUnpacker wrapper for parsed wikipedia corpus.
 *
 * Created by zhuoranzhang on 5/27/16.
 */
public class SentenceStream {

    private MessageUnpacker unpacker;
    private int numSentences;
    private int numRead;

    public static SentenceStream getStream(String path) {
        try {
            return new SentenceStream(path);
        } catch (IOException e) {
            System.out.println("Unable to open sentence stream.");
        }
        return null;
    }

    private SentenceStream(String path) throws IOException {
        unpacker = MessagePack.newDefaultUnpacker(new GZIPInputStream(new FileInputStream(path)));
        numSentences = (unpacker.unpackArrayHeader());
        numRead = 0;
    }

    public synchronized Sentence getSentence() {
        if (numRead == numSentences) {
            return null;
        }
        numRead++;
        System.out.println(String.format("Processing sentence %d", numRead));
        try {
            unpacker.unpackArrayHeader(); // Unpack the sentence array header.
            int numWords = unpacker.unpackArrayHeader();
            for (int i = 0; i < numWords; i++) {
                unpacker.unpackString();
            }
            unpacker.unpackArrayHeader(); // Unpack lemmas array header.
            String[] words = new String[numWords];
            for (int i = 0; i < numWords; i++) {
                words[i] = unpacker.unpackString().toLowerCase();
            }
            unpacker.unpackArrayHeader(); // Unpack POS array header.
            for (int i = 0; i < numWords; i++) {
                unpacker.unpackString();
            }
            int numDeps = unpacker.unpackArrayHeader();
            for (int i = 0; i < numDeps; i++) {
                unpacker.unpackArrayHeader();
                unpacker.unpackString();
                unpacker.unpackInt();
                unpacker.unpackInt();
            }
            int numCollapsedDeps = unpacker.unpackArrayHeader();
            int[][] dependencies = new int[numCollapsedDeps - 1][2];
            // Skip the first dependency, assuming the ROOT dependency is always first.
            unpacker.unpackArrayHeader();
            unpacker.unpackString();
            unpacker.unpackInt();
            unpacker.unpackInt();
            for (int i = 0; i < numCollapsedDeps - 1; i++) {
                unpacker.unpackArrayHeader();
                unpacker.unpackString();
                dependencies[i][0] = unpacker.unpackInt() - 1;
                dependencies[i][1] = unpacker.unpackInt() - 1;
            }
            return new Sentence(words, dependencies);
        } catch (IOException e) {
            return null;
        }
    }

}

