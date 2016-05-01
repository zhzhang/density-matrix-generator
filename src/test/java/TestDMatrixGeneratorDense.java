import dmatrix.DMatrixGeneratorDense;
import org.junit.Test;

import java.net.URL;

/**
 * Created by zhuoranzhang on 5/1/16.
 */
public class TestDMatrixGeneratorDense {
    @Test
    public void testMatrixGeneration() {
        URL testData = this.getClass().getResource("/test_data.txt");
        URL testTargets = this.getClass().getResource("/test_targets.txt");
        URL testVectors = this.getClass().getResource("/test_vectors.txt");
        float[] alpha = new float[]{0.0f, 0.1f, 0.2f, 0.3f};
        float[] beta = new float[]{0.0f, 0.0f, 0.2f, 0.3f};
        float[] gamma = new float[]{0.0f, -0.1f, 0.2f, 0.3f};
        // Test for case where alpha is not included in context.
        float[][] trueMatrix = TestUtils.matrixSum(TestUtils.outerProduct(
                TestUtils.vectorSum(TestUtils.vectorScalarProduct(2.0f, beta),
                        TestUtils.vectorScalarProduct(3.0f, gamma))),
                TestUtils.matrixScalarProduct(2.0f,
                        TestUtils.outerProduct(TestUtils.vectorSum(TestUtils.vectorScalarProduct(2.0f, beta),
                                TestUtils.vectorScalarProduct(2.0f, gamma)))));
        DMatrixGeneratorDense dmg = new DMatrixGeneratorDense(testData.getPath(), testTargets.getPath(),
                2, testVectors.getPath(), 1);
        dmg.generateMatrices();
        TestUtils.assert2DFloatArrayEquals(trueMatrix, dmg.getMatrix("alpha"));
        // Test for case where alpha is included in context.
        trueMatrix = TestUtils.matrixSum(
                TestUtils.outerProduct(
                        TestUtils.vectorSum(
                                TestUtils.vectorScalarProduct(2.0f, beta),
                                TestUtils.vectorScalarProduct(3.0f, gamma))),
                TestUtils.matrixScalarProduct(2.0f, TestUtils.outerProduct(
                        TestUtils.vectorSum(alpha, TestUtils.vectorSum(
                                TestUtils.vectorScalarProduct(2.0f, beta),
                                TestUtils.vectorScalarProduct(2.0f, gamma))))));
        dmg = new DMatrixGeneratorDense(testData.getPath(), testTargets.getPath(),
                3, testVectors.getPath(), 1);
        dmg.generateMatrices();
        TestUtils.assert2DFloatArrayEquals(trueMatrix, dmg.getMatrix("alpha"));
    }
}
