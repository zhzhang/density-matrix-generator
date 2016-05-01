package dmatrix.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

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
            List<String> result = new ArrayList<String>();
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

    public static FileOutputStream getOutputStream(String outputDir, String target) {
        File outputDirFile = new File(outputDir);
        if (!outputDirFile.exists()) {
            outputDirFile.mkdirs();
        }
        FileOutputStream outputStream = null;
        try {
            outputStream = new FileOutputStream(Paths.get(outputDir, target + ".dat").toString());
        } catch (FileNotFoundException e) {
            System.out.println("Failed to write matrices to output.");
        }
        return outputStream;
    }
}
