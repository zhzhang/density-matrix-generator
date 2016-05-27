package dmatrix.io;

/**
 * Wrapper object for sentence data.
 *
 * Created by zhuoranzhang on 5/27/16.
 */
public class Sentence {

    final String[] words;
    final int[][] dependencies;

    Sentence(String[] words, int[][] dependencies) {
        this.words = words;
        this.dependencies = dependencies;
    }

    public int size() {
        return words.length;
    }

    public int[][] getDependencies() {
        return dependencies;
    }

    public String getWord(int index) {
        return words[index];
    }

}
