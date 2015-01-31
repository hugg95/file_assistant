package org.lncwwn.assistant.util;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVWriter;

import java.io.*;
import java.math.BigDecimal;
import java.util.DoubleSummaryStatistics;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
            try {
                BufferedReader bf = new BufferedReader(
                        new InputStreamReader(
                                new FileInputStream(file)));
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
            }
        }

    }

    public void analyse(BlockingQueue<String> queue1, BlockingQueue<String> queue2) {
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
                        //sum += Double.parseDouble(data[14]);
                        sum = sum.add(new BigDecimal(value));
                    }
                } else {
                    String result = key + "," + sum + "\n";
                    System.out.println("-------------------"+result);
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
            File child = new File(currentPath + "/gen.csv");
            BufferedWriter bufferedWriter = null;
            CSVWriter csvWriter = null;
            try {
                bufferedWriter = new BufferedWriter(
                        new OutputStreamWriter(
                                new FileOutputStream(child)));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            if (null != bufferedWriter) {
                csvWriter = new CSVWriter(bufferedWriter);
                String line = null;
                try {
                    line = queue.take();
                    System.out.println(line);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                while (null != line) {
                    if (offset < limit) {
                        //bufferedWriter.write(line);
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
                }
                try {
                    csvWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * writes content from queue into file by the specified file path.
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
            write(queue, filePath, 0, 3000);
        } else {
            String line = null;
            try {
                line = queue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            BufferedWriter bufferedWriter = null;
            CSVWriter csvWriter = null;
            try {
                bufferedWriter = new BufferedWriter(
                        new OutputStreamWriter(
                                new FileOutputStream(file)));
                csvWriter = new CSVWriter(bufferedWriter);
                while (null != line) {
                    //bufferedWriter.write(line);
                    csvWriter.writeNext(line.split(","));
                    try {
                        line = queue.take();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                csvWriter.close();

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

    protected boolean isNumeric(String input) {
        Pattern pattern = Pattern.compile("^(\\d+|(\\d+\\.\\d+))$");
        Matcher matcher = pattern.matcher(input);
        return matcher.matches();
    }

}
