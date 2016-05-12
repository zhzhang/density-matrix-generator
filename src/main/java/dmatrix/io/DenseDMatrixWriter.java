package dmatrix.io;

import java.io.IOException;

/**
 * Serializer for dense density matrices.
 * <p>
 * Created by zhuoranzhang on 5/12/16.
 */
public class DenseDMatrixWriter extends DMatrixWriter {

    public DenseDMatrixWriter(String word, String matricesPath) {
        super(word, matricesPath);
    }

    public void writeMatrix(float[][] matrix) {
        try {
            for (float[] row : matrix) {
                for (float value : row) {
                    outputStream.write(float2ByteArray(value));
                }
            }
        } catch (IOException e) {
            System.out.println(String.format("IOException thrown for word: %s. Matrix potentially corrupted.", word));
        }
    }

}
