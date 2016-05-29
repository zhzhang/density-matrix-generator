package dmatrix.io;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Wrapper object for sentence data.
 * <p>
 * Created by zhuoranzhang on 5/27/16.
 */
public class Sentence {

    private final Map<Integer, String> words;
    private final List<Integer[]> dependencies;

    Sentence(Map<Integer, String> words, List<Integer[]> dependencies) {
        this.words = words;
        this.dependencies = dependencies;
    }

    public int size() {
        return words.size();
    }

    public List<Integer[]> getDependencies() {
        return dependencies;
    }

    public String getWord(int index) {
        return words.get(index);
    }

    public Set<String> getWords() {
        return words.values().stream().collect(Collectors.toSet());
    }

}
