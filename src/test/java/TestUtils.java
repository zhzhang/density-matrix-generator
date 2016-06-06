import org.junit.Assert;

/**
 * Static utilities for conducting tests.
 * <p>
 * Created by zhuoranzhang on 5/1/16.
 */
class TestUtils {

    private static final float TOLERANCE = 1e-6f;

    static float[][] outerProduct(float[] vector) {
        float[][] output = new float[vector.length][vector.length];
        for (int i = 0; i < vector.length; i++) {
            for (int j = i; j < vector.length; j++) {
                output[i][j] = output[j][i] = vector[i] * vector[j];
            }
        }
        return output;
    }

    static float[] normalize(float[] vector) {
        double norm = 0.0f;
        for (float value : vector) {
            norm += value * value;
        }
        norm = Math.sqrt(norm);
        float[] output = new float[vector.length];
        for (int i = 0; i < vector.length; i++) {
            output[i] = vector[i] / (float) norm;
        }
        return output;
    }

    static float[][] matrixSum(float[][] X, float[][] Y) {
        float[][] output = new float[X.length][X.length];
        for (int i = 0; i < X.length; i++) {
            for (int j = i; j < X.length; j++) {
                output[i][j] = output[j][i] = X[i][j] + Y[i][j];
            }
        }
        return output;
    }

    static float[][] matrixScalarProduct(float scalar, float[][] matrix) {
        float[][] output = new float[matrix.length][matrix.length];
        for (int i = 0; i < matrix.length; i++) {
            for (int j = i; j < matrix.length; j++) {
                output[i][j] = output[j][i] = scalar * matrix[i][j];
            }
        }
        return output;
    }

    static float[] vectorSum(float[] x, float[] y)  {
        float[] output = new float[x.length];
        for (int i = 0; i < x.length; i++) {
            output[i] = x[i] + y[i];
        }
        return output;
    }

    static float[] vectorScalarProduct(float scalar, float[] x)  {
        float[] output = new float[x.length];
        for (int i = 0; i < x.length; i++) {
            output[i] = scalar * x[i];
        }
        return output;
    }

    static void assert2DFloatArrayEquals(float[][] X, float[][] Y) {
        for (int i = 0; i < X.length; i++) {
            Assert.assertArrayEquals(X[i], Y[i], TOLERANCE);
        }
    }

}
