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
        stopWords = new HashSet<>(Arrays.asList(
                new String[]{"i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours",
                        "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers",
                        "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves",
                        "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are",
                        "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does",
                        "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until",
                        "while", "of", "at", "by", "for", "with", "about", "against", "between", "into",
                        "through", "during", "before", "after", "above", "below", "to", "from", "up", "down",
                        "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here",
                        "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more",
                        "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so",
                        "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now", "lrb", "rrb"}));
    }

    public TokenizedFileReaderFactory(String stopWordsPath) {
        loadStopWords(stopWordsPath);
    }

    private void loadStopWords(String stopWordsPath) {
        stopWords = new HashSet<>();
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
