package dmatrix.io;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class IOUtils {
    public static String getFileExtension(String fileName) {
        if (fileName.lastIndexOf(".") != -1 && fileName.lastIndexOf(".") != 0)
            return fileName.substring(fileName.lastIndexOf(".") + 1);
        else return "";
    }

    public static List<String> getFilePaths(String path) {
        File f = new File(path);
        if (f.isFile()) {
            return Arrays.asList(path);
        } else if (f.isDirectory()) {
            List<String> result = new ArrayList<>();
            for (File child : f.listFiles()) {
                result.addAll(getFilePaths(child.getPath()));
            }
            return result;
        }
        return null;
    }

    public static List<List<String>> getFilePathPartitions(String root, int numPartitions) {
        List<String> filePaths = IOUtils.getFilePaths(root);
        int partitionSize = (int) Math.ceil((float) filePaths.size() / numPartitions);
        List<List<String>> filePathPartitions = new ArrayList<List<String>>();
        for (int i = 0; i < filePaths.size(); i += partitionSize) {
            filePathPartitions.add(filePaths.subList(i, Math.min(i + partitionSize, filePaths.size())));
        }
        return filePathPartitions;
    }

    public static float[][] loadSparseMatrix(String path, int dim) throws IOException {
        float[][] output = new float[dim][dim];
        byte[] bytes = org.apache.commons.io.IOUtils.toByteArray(new FileInputStream(new File(path)));
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        int location = 0;
        while (location < byteBuffer.capacity()) {
            int x = byteBuffer.getInt(location);
            int y = byteBuffer.getInt(location + 4);
            float value = byteBuffer.getFloat(location + 8);
            output[x][y] = output[y][x] = value;
            location += 12;
        }
        return output;
    }

}
