package dmatrix.io;

import java.io.IOException;
import java.util.Set;

/**
 * Created by zhuoranzhang on 5/29/16.
 */
public class SentenceStreamFactory {

    private Set<String> targets;

    public SentenceStreamFactory(Set<String> targets) {
        this.targets = targets;
    }

    public SentenceStream getStream(String path) {
        try {
            return new SentenceStream(path, targets);
        } catch (IOException e) {
            System.out.println(String.format("Unable to open sentence stream for file %s, %s.", path, e.getMessage()));
        }
        return null;
    }


}
