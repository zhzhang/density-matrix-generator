package dmatrix.io;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Paths;

/**
 * Serializer for sparse density matrices.
 * <p>
 * Created by zhuoranzhang on 5/12/16.
 */
public class SparseDMatrixWriter {
    private BufferedOutputStream outputStream;
    private String word;

    public SparseDMatrixWriter(String word, String matricesPath) {
        this.word = word;
        try {
            File outputFile = new File(Paths.get(matricesPath, word + ".bin").toString());
            if (!outputFile.exists()) {
                if (!outputFile.getParentFile().exists()) {
                    outputFile.getParentFile().mkdirs();
                }
                outputFile.createNewFile();
            }
            outputStream = new BufferedOutputStream(new FileOutputStream(outputFile, false));
        } catch (FileNotFoundException e) {
            System.out.println(String.format("Unable to open file for word: %s.", word));
        } catch (IOException e) {
            System.out.println(String.format("Unable to create file for word: %s.", word));
        }
    }

    public void writeEntry(int x, int y, float value) {
        try {
            outputStream.write(int2ByteArray(x));
            outputStream.write(int2ByteArray(y));
            outputStream.write(float2ByteArray(value));
        } catch (IOException e) {
            System.out.println(String.format("IOException thrown for word: %s. Matrix potentially corrupted.", word));
        }
    }

    public void close() {
        try {
            outputStream.close();
        } catch (IOException e) {
            System.out.println(String.format("IOException thrown for word: %s. Matrix potentially corrupted.", word));
        }
    }

    private static byte[] int2ByteArray(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    private static byte[] float2ByteArray(float value) {
        return ByteBuffer.allocate(4).putFloat(value).array();
    }
}
