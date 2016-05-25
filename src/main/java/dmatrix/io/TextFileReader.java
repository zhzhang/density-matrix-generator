package dmatrix.io;

import java.io.*;
import java.util.zip.GZIPInputStream;

/**
 * Created by zhuoranzhang on 4/28/16.
 *
 * Easy file reader for reading plain text files or those compressed with gz.
 */
public class TextFileReader {

    private BufferedReader bufferedReader;

    public TextFileReader(String filePath) {
        try {
            if (IOUtils.getFileExtension(filePath).equals("gz")) {
                GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(filePath));
                bufferedReader = new BufferedReader(new InputStreamReader(gzip));
            } else {
                bufferedReader = new BufferedReader(new FileReader(filePath));
        }
    } catch (FileNotFoundException e) {
        e.printStackTrace();
    } catch (IOException e) {
        e.printStackTrace();
        }
    }

    public String readLine() {
        try {
            return bufferedReader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void close() {
        try {
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
