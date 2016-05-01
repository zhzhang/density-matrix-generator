import dmatrix.DMatrixGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;

/**
 * Test matrix generation.
 * <p>
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
        float[] context1 = new float[]{3, 2, 0};
        float[] context2 = new float[]{2, 2, 1};
        trueMatrix = TestUtils.sum(TestUtils.outerProduct(context1),
                TestUtils.scalarProduct(2.0f, TestUtils.outerProduct(context2)));
        dmg = new DMatrixGenerator(testData.getPath(), testTargets.getPath(), 3, 1);
        dmg.generateMatrices();
        matrix = dmg.getMatrix("alpha");
        Assert.assertArrayEquals(matrix, trueMatrix);
    }


}
