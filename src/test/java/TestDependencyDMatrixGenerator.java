import dmatrix.DependencyDMatrixGenerator;
import dmatrix.io.IOUtils;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.fail;

/**
 * Test matrix generation.
 * <p>
 * Created by zhuoranzhang on 4/29/16.
 */
public class TestDependencyDMatrixGenerator {

    @Test
    public void testMatrixGeneration() {
        URL testData = this.getClass().getResource("/test-data-parsed");

        // Test for case where alpha is not included in context.
        float[][] trueMatrix = new float[2][2];
        trueMatrix[0][0] = 17.0f;
        trueMatrix[1][1] = 12.0f;
        trueMatrix[0][1] = trueMatrix[1][0] = 14.0f;
        Set<String> targets = new HashSet<>(Arrays.asList(new String[]{"alpha"}));
        DependencyDMatrixGenerator dmg
                = new DependencyDMatrixGenerator(testData.getPath(), targets, 2, 2, false);
        dmg.generateMatrices();
        float[][] matrix = dmg.getMatrix("alpha");
        Assert.assertArrayEquals(matrix, trueMatrix);

        // Test for case where alpha is included in context.
        float[] context1 = new float[]{3, 2, 0};
        float[] context2 = new float[]{2, 2, 1};
        trueMatrix = TestUtils.matrixSum(TestUtils.outerProduct(context1),
                TestUtils.matrixScalarProduct(2.0f, TestUtils.outerProduct(context2)));
        dmg = new DependencyDMatrixGenerator(testData.getPath(), targets, 0, 2, false);
        dmg.generateMatrices();
        matrix = dmg.getMatrix("alpha");
        Assert.assertArrayEquals(matrix, trueMatrix);

        // Test writing to file.
        String outputDir = String.format("tmp_test_matrices_%d", System.nanoTime() / 1000000000);
        dmg.writeMatrices(outputDir);
        matrix = new float[0][0];
        try {
            matrix = IOUtils.loadSparseMatrix(outputDir + "/alpha.bin", 3);
        } catch (FileNotFoundException e) {
            Assert.fail("FileNotFoundException was thrown.");
        } catch (IOException e) {
            Assert.fail("IOException was thrown.");
        }
        Assert.assertArrayEquals(matrix, trueMatrix);
        // Cleanup
        try {
            FileUtils.deleteDirectory(new File(outputDir));
        } catch (IOException e) {
            Assert.fail("Failed to delete test output directory.");
        }
    }

    @Test
    public void testVectorGeneration() {
        URL testData = this.getClass().getResource("/test-data-parsed");
        Set<String> targets = new HashSet<>(Arrays.asList(new String[]{"alpha"}));
        DependencyDMatrixGenerator dmg = new DependencyDMatrixGenerator(testData.getPath(), targets, 3, 2, true);
        dmg.generateMatrices();
        float[] vector = dmg.getVector("alpha");
        float[] trueVector;
        trueVector = new float[]{7.0f, 6.0f, 2.0f};
        Assert.assertArrayEquals(vector, trueVector, 0.0f);
    }


}
