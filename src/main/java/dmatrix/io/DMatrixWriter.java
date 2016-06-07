package dmatrix.io;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Paths;

/**
 * Base serializer for density matrices.
 * <p>
 * Created by zhuoranzhang on 5/12/16.
 */
public abstract class DMatrixWriter {
    BufferedOutputStream outputStream;
    String word;

    public DMatrixWriter(String word, String matricesPath) {
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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            outputStream.close();
        } catch (IOException e) {
            System.out.println(String.format("IOException thrown for word: %s. Matrix potentially corrupted.", word));
        }
    }

    static byte[] int2ByteArray(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    static byte[] float2ByteArray(float value) {
        return ByteBuffer.allocate(4).putFloat(value).array();
    }
}
