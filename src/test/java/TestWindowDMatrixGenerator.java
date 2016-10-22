import dmatrix.SentenceDMatrixGenerator;
import dmatrix.WindowDMatrixGenerator;
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

/**
 * Test matrix generation.
 * <p>
 * Created by zhuoranzhang on 4/29/16.
 */
public class TestWindowDMatrixGenerator {

    @Test
    public void testMatrixGeneration() {
        URL testData = this.getClass().getResource("/test-data");

        // Test for case where alpha is not included in context.
        float[][] trueMatrix = new float[2][2];
        trueMatrix[0][0] = 9.0f;
        trueMatrix[1][1] = 0.0f;
        trueMatrix[0][1] = trueMatrix[1][0] = 0.0f;
        Set<String> targets = new HashSet<>(Arrays.asList(new String[]{"alpha"}));
        WindowDMatrixGenerator dmg = new WindowDMatrixGenerator(testData.getPath(), targets, 2, 2, false, 2);
        dmg.generateMatrices();
        float[][] matrix = dmg.getMatrix("alpha");
        Assert.assertArrayEquals(matrix, trueMatrix);
    }

}
