package dmatrix.io;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by zhuoranzhang on 4/28/16.
 *
 * Reads text files and returns tokens line by line.
 */
public class TokenizedFileReader extends TextFileReader {
    private Set<String> stopWords;

    public TokenizedFileReader(String path, Set<String> stopWords) {
        super(path);
        this.stopWords = stopWords;
    }
    public String[] readLineTokens() {
        String line = super.readLine();
        if (line == null) return null;
        return tokenizeLine(line);
    }

    public String[] tokenizeLine(String line) {
        if (line.startsWith("<doc") || line.startsWith("</doc")) {
            return new String[0];
        }
        String[] tokens = line.replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase().split("\\s+");
        List<String> output = new ArrayList<>();
        for (String token : tokens) {
            if (token.length() == 0
                    || (!(stopWords == null) && stopWords.contains(token))
                    || token.matches(".*\\d.*")) {
                continue;
            }
            output.add(token);
        }
        return output.toArray(new String[output.size()]);
    }

}
