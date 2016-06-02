import dmatrix.SentenceDMatrixGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;

/**
 * Test matrix generation.
 * <p>
 * Created by zhuoranzhang on 4/29/16.
 */
public class TestSentenceDMatrixGenerator {

    @Test
    public void testMatrixGeneration() {
        URL testData = this.getClass().getResource("/test-data");
        URL testTargets = this.getClass().getResource("/test_targets.txt");

        // Test for case where alpha is not included in context.
        float[][] trueMatrix = new float[2][2];
        trueMatrix[0][0] = 34.0f;
        trueMatrix[1][1] = 17.0f;
        trueMatrix[0][1] = trueMatrix[1][0] = 24.0f;
        SentenceDMatrixGenerator dmg = new SentenceDMatrixGenerator(testData.getPath(), testTargets.getPath(), 2, 2, false);
        dmg.generateMatrices();
        float[][] matrix = dmg.getMatrix("alpha");
        Assert.assertArrayEquals(matrix, trueMatrix);

        // Test for case where alpha is included in context.
        float[] context1 = new float[]{4, 3, 0};
        float[] context2 = new float[]{3, 2, 1};
        trueMatrix = TestUtils.matrixSum(TestUtils.outerProduct(context1),
                TestUtils.matrixScalarProduct(2.0f, TestUtils.outerProduct(context2)));
        dmg = new SentenceDMatrixGenerator(testData.getPath(), testTargets.getPath(), 0, 2, false);
        dmg.generateMatrices();
        matrix = dmg.getMatrix("alpha");
        Assert.assertArrayEquals(matrix, trueMatrix);

        // Test writing to file.
        /*String outputDir = String.format("tmp_test_matrices_%d", System.nanoTime() / 1000000000);
        dmg.writeMatrices(outputDir);
        matrix = new float[0][0];
        try {
            DMatrixSparse matrixProto = DMatrixSparse.parseFrom(new FileInputStream(outputDir + "/alpha.dat"));
            matrix = new float[matrixProto.getDimension()][matrixProto.getDimension()];
            for (DMatrixSparse.DMatrixEntry entry : matrixProto.getEntriesList()) {
                int x = entry.getX();
                int y = entry.getY();
                matrix[x][y] = matrix[y][x] = entry.getValue();
            }
        } catch (FileNotFoundException e) {
            Assert.fail("FileNotFoundException was thrown.");
        } catch (IOException e) {
            Assert.fail("IOException was thrown.");
        }
        Assert.assertArrayEquals(matrix, trueMatrix);
        // Cleanup
        (new File(outputDir + "/alpha.dat")).delete();
        (new File(outputDir)).delete();*/
    }


}
