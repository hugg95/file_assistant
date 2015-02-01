package org.lncwwn.assistant.exec;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVWriter;

import java.io.*;
import java.math.BigDecimal;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by victor on 2/1/15.
 */
public class Main2 {

    private static final short KEY_INDEX = 1;
    private static final short VALUE_INDEX = 20;

    public static void main(String[] args) {
        String path = args[0];
        Map<String, List<String>> lines = new HashMap<String, List<String>>();
        Map<String, Map<String, BigDecimal>> result = new HashMap<String, Map<String, BigDecimal>>();
        System.out.println("start to read files, please wait...");
        read(lines, path);
        System.out.println("reading finished, start parse each line now...");
        parse(lines, result);
        System.out.println("parsing finished, start to write result into files now...");
        write(result, path);
        System.out.println("writing finished, please close the window");
    }

    public static void read(Map<String, List<String>> lines, String path) {
        File file = new File(path);
        if (file.exists()) {
            if (file.isDirectory()) {
                File[] files = file.listFiles();
                for (File f : files) {
                    read(lines, f.getAbsolutePath());
                }
            } else {
                String name = file.getName();
                int first = name.indexOf('_');
                int last = name.indexOf('.');
                if (first == -1 || last == -1) {
                    return;
                }
                String key = name.substring(first, last);
                List<String> list = lines.get(key);
                if (null == list) {
                    list = new ArrayList<String>();
                    lines.put(key, list);
                }
                BufferedReader reader = null;
                try {
                    reader = new BufferedReader(
                            new InputStreamReader(
                                    new FileInputStream(file)));
                    String line = reader.readLine();
                    while (null != line) {
                        list.add(line);
                        line = reader.readLine();
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    if (null != reader) {
                        try {
                            reader.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    public static void parse(Map<String, List<String>> in, Map<String, Map<String, BigDecimal>> out) {
        if (null != in) {
            CSVParser parse = new CSVParser();
            Iterator<Map.Entry<String, List<String>>> iterator
                    = in.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, List<String>> set = iterator.next();
                String key = set.getKey();
                List<String> value = set.getValue();
                if (null != value && !value.isEmpty()) {
                    ListIterator<String> listIterator = value.listIterator();
                    Map<String, BigDecimal> result = new HashMap<String, BigDecimal>();
                    while (listIterator.hasNext()) {
                        String line = listIterator.next();
                        try {
                            String[] data = parse.parseLine(line);
                            if (data.length < KEY_INDEX || data.length < VALUE_INDEX) {
                                continue;
                            }
                            String year = data[KEY_INDEX];
                            String flow = data[VALUE_INDEX];
                            if (!isNumeric(flow)) {
                                continue;
                            }
                            BigDecimal sum = result.get(year);
                            if (null == sum) {
                                sum = new BigDecimal(0);
                            }
                            sum = sum.add(new BigDecimal(flow));
                            result.put(year, sum);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    out.put(key, result);
                }
            }
        }
    }

    public static void write(Map<String, Map<String, BigDecimal>> in, String path) {
        Iterator<Map.Entry<String, Map<String, BigDecimal>>> iterator
                = in.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Map<String,BigDecimal>> item = iterator.next();
            String key = item.getKey();
            Map<String, BigDecimal> value = item.getValue();
            if ('/' != path.charAt(path.length() - 1)) {
                path = path + "/";
            }
            File file = new File(path + "sum" + key + ".csv");
            if (!file.exists()) {
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (file.exists()) {
                CSVWriter writer = null;
                try {
                    writer = new CSVWriter(new FileWriter(file));
                    Iterator<Map.Entry<String, BigDecimal>> _iterator
                            = value.entrySet().iterator();
                    while (_iterator.hasNext()) {
                        Map.Entry<String, BigDecimal> next = _iterator.next();
                        String year = next.getKey();
                        BigDecimal sum = next.getValue();
                        writer.writeNext(new String[] {year, sum.toString()});
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    if (writer != null) {
                        try {
                            writer.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }

        }
    }

    protected static boolean isNumeric(String input) {
        Pattern pattern = Pattern.compile("^(\\d+|(\\d+\\.\\d+))$");
        Matcher matcher = pattern.matcher(input);
        return matcher.matches();
    }

}
