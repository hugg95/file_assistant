package org.lncwwn.file_assistant.util;

import java.io.*;
import java.util.Queue;

/**
 * An assistant of file operation.
 * @author victor.li
 * @date 2015-01-29
 */
public class FileAssistant {

    /**
     * reads lines from files and put these lines into queue.
     * @param queue
     * @param path
     */
    public void reader(Queue<String> queue, String path) {
        File file = new File(path);
        if (!file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File f : files) {
                if (f.isDirectory()) {
                    reader(queue, file.getAbsolutePath());
                }
            }
        } else {
            try {
                BufferedReader bf = new BufferedReader(
                        new InputStreamReader(
                                new FileInputStream(file)));
                String line = bf.readLine();
                while (null != line) {
                    queue.add(line);
                    line = bf.readLine();
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * writes content from queue into file by the specified dir path.
     * @param queue
     * @param dirPath
     * @param maxLine
     */
    public void write(Queue<String> queue, String dirPath, int maxLine) {
        File dir = new File(dirPath);
        if (!dir.exists()) {
            try {
                dir.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (dir.isFile()) {
            write(queue, dirPath);
        } else {
            String currentPath = dir.getAbsolutePath();
            File child = new File(currentPath + "/gen.csv");
        }
    }

    /**
     * writes content from queue into file by the specified file path.
     * @param queue
     * @param filePath
     */
    public void write(Queue<String> queue, String filePath) {
        File file = new File(filePath);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (file.isDirectory()) {
            write(queue, filePath, 3000);
        } else {
            String line = queue.poll();
            BufferedWriter bufferedWriter = null;
            try {
                bufferedWriter = new BufferedWriter(
                        new OutputStreamWriter(
                                new FileOutputStream(file)));
                while (null != line) {
                    bufferedWriter.write(line);
                    line = queue.poll();
                }

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    bufferedWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

}
