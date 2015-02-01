package org.lncwwn.assistant.util;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVWriter;

import java.io.*;
import java.math.BigDecimal;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An assistant of file operation.
 *
 * @author victor.li
 * @date 2015-01-29
 */
public class FileAssistant {

    /**
     * reads lines from files and put these lines into queue.
     *
     * @param queue
     * @param path
     */
    public void reader(BlockingQueue<String> queue, String path) {
        File file = new File(path);
        if (!file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File f : files) {
                System.out.println(f.getAbsolutePath());
                reader(queue, f.getAbsolutePath());
            }
        } else {
            BufferedReader bf = null;
            try {
                bf = new BufferedReader(
                        new InputStreamReader(
                                new FileInputStream(file), "utf-8"));
                String line = bf.readLine();
                while (null != line) {
                    queue.put(line);
                    line = bf.readLine();
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                if (null != bf) {
                    try {
                        bf.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

    }

    public void parse(BlockingQueue<String> queue1, BlockingQueue<String> queue2) {
        String line = null;
        try {
            line = queue1.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        String key = "";
        BigDecimal sum = new BigDecimal(0);
        CSVParser csvParser = new CSVParser();
        while (null != line) {
            try {
                String[] data = csvParser.parseLine(line);
                String nextKey = data[1];
                if (key.isEmpty()) {
                    key = nextKey;
                }
                if (key.equals(nextKey)) {
                    String value = data[14];
                    if (isNumeric(value)) {
                        sum = sum.add(new BigDecimal(value));
                    }
                } else {
                    String result = key + "," + sum;
                    queue2.put(result);
                    key = nextKey;
                    sum = new BigDecimal(0);
                }
                line = queue1.take();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * writes content from queue into file by the specified dir path.
     *
     * @param queue
     * @param dirPath
     * @param limit
     */
    public void write(BlockingQueue<String> queue, String dirPath, int offset, int limit) {
        File dir = new File(dirPath);
        if (!dir.exists()) {
            dir.mkdir();
        }
        if (dir.isFile()) {
            write(queue, dirPath);
        } else {
            String currentPath = dir.getAbsolutePath();
            File child = new File(currentPath + "/sum.csv");
            BufferedWriter bufferedWriter = null;
            FileWriter writer = null;
            try {
                bufferedWriter = new BufferedWriter(
                        new OutputStreamWriter(
                                new FileOutputStream(child), "utf-8"));
                writer = new FileWriter(child.getAbsolutePath());
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            CSVWriter csvWriter = null;
            if (null != bufferedWriter) {
                csvWriter = new CSVWriter(writer);
            }
            try {
                String line = queue.take();
                while (null != line) {
                    if (offset < limit) {
                        csvWriter.writeNext(line.split(","));
                    } else {
                        offset = 0;
                        write(queue, currentPath, offset, limit);
                    }
                    offset++;
                    try {
                        line = queue.take();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    csvWriter.flush();
                }
                if (null != csvWriter) {
                    csvWriter.flush();
                    csvWriter.close();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (null != csvWriter) {
                    try {
                        csvWriter.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    /**
     * writes content from queue into file by the specified file path.
     *
     * @param queue
     * @param filePath
     */
    public void write(BlockingQueue<String> queue, String filePath) {
        File file = new File(filePath);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (file.isDirectory()) {
            write(queue, filePath, 0, 600);
        } else {
            CSVWriter csvWriter = null;
            try {
                String line = queue.take();
                FileWriter writer = new FileWriter(file);
                csvWriter = new CSVWriter(writer);
                while (null != line) {
                    System.out.println("=================" + line);
                    csvWriter.writeNext(line.split(","));
                    try {
                        line = queue.take();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    csvWriter.flush();
                }
                if (null != csvWriter) {
                    csvWriter.flush();
                    csvWriter.close();
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (null != csvWriter) {
                        csvWriter.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    protected boolean isNumeric(String input) {
        Pattern pattern = Pattern.compile("^(\\d+|(\\d+\\.\\d+))$");
        Matcher matcher = pattern.matcher(input);
        return matcher.matches();
    }

}
