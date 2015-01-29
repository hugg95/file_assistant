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
     * writes content from queue into file by the specified path.
     * @param queue
     * @param path
     */
    public void write(Queue<String> queue, String path) {
        File file = new File(path);
        if (!file.exists()) {

        }
    }

}
