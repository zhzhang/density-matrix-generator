package dmatrix.io;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by zhuoranzhang on 4/28/16.
 */
public class TokenizedFileReaderFactory {
    private Set<String> stopWords;

    public TokenizedFileReaderFactory() {
        stopWords = new HashSet<String>(Arrays.asList(
                new String[] {"a", "an", "and", "are", "as", "at", "be", "but", "by",
                        "for", "if", "in", "into", "is", "it", "no", "not", "of", "on",
                        "or", "such", "that", "the", "their", "then", "there", "these",
                        "they", "this", "to", "was", "will", "with"}));
    }

    public TokenizedFileReaderFactory(String stopWordsPath) {
        loadStopWords(stopWordsPath);
    }

    private void loadStopWords(String stopWordsPath) {
        stopWords = new HashSet<String>();
        TextFileReader reader = new TextFileReader(stopWordsPath);
        String word;
        while ((word = reader.readLine()) != null) {
            stopWords.add(word);
        }
    }

    public TokenizedFileReader getReader(String path) {
        return new TokenizedFileReader(path, stopWords);
    }

}
