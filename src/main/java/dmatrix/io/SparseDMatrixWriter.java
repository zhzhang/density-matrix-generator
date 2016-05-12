package dmatrix.io;

import java.io.IOException;

/**
 * Serializer for sparse density matrices.
 * <p>
 * Created by zhuoranzhang on 5/12/16.
 */
public class SparseDMatrixWriter extends DMatrixWriter {

    public SparseDMatrixWriter(String word, String matricesPath) {
        super(word, matricesPath);
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

}
