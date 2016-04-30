import dmatrix.DMatrixGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;

/**
 * Test matrix generation.
 *
 * Created by zhuoranzhang on 4/29/16.
 */
public class TestDMatrixGenerator {

    @Test
    public void testMatrixGeneration() {
        URL testData = this.getClass().getResource("/test_data.txt");
        URL testTargets = this.getClass().getResource("/test_targets.txt");
        // Test for case where alpha is not included in context.
        float[][] trueMatrix = new float[2][2];
        trueMatrix[0][0] = 17.0f;
        trueMatrix[1][1] = 12.0f;
        trueMatrix[0][1] = trueMatrix[1][0] = 14.0f;
        DMatrixGenerator dmg = new DMatrixGenerator(testData.getPath(), testTargets.getPath(), 2, 1);
        dmg.generateMatrices();
        float[][] matrix = dmg.getMatrix("alpha");
        Assert.assertArrayEquals(matrix, trueMatrix);
        // Test for case where alpha is included in context.
        float[] context1 = new float[] {3, 2, 0};
        float[] context2 = new float[] {2, 2, 1};
        trueMatrix = sum(outerProduct(context1), scalarProduct(2.0f, outerProduct(context2)));
        dmg = new DMatrixGenerator(testData.getPath(), testTargets.getPath(), 3, 1);
        dmg.generateMatrices();
        matrix = dmg.getMatrix("alpha");
        Assert.assertArrayEquals(matrix, trueMatrix);
    }

    private static float[][] outerProduct(float[] vector) {
        float[][] output = new float[vector.length][vector.length];
        for (int i = 0; i < vector.length; i++) {
            for (int j = i; j < vector.length; j++) {
                output[i][j] = output[j][i] = vector[i] * vector[j];
            }
        }
        return output;
    }

    private static float[][] sum(float[][] X, float[][] Y) {
        float[][] output = new float[X.length][X.length];
        for (int i = 0; i < X.length; i++) {
            for (int j = i; j < X.length; j++) {
                output[i][j] = output[j][i] = X[i][j] + Y[i][j];
            }
        }
        return output;
    }

    private static float[][] scalarProduct(float scalar, float[][] matrix) {
        float[][] output = new float[matrix.length][matrix.length];
        for (int i = 0; i < matrix.length; i++) {
            for (int j = i; j < matrix.length; j++) {
                output[i][j] = output[j][i] = scalar * matrix[i][j];
            }
        }
        return output;
    }


}
