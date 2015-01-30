package org.lncwwn.file_assistant.exec;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by victor on 1/29/15.
 */
public class Executor {

    public static void main(String[] args) {
        Queue<String> queue = new ArrayBlockingQueue<String>(1000);
        // TODO
        ExecutorService readerThreads = Executors.newFixedThreadPool(3);
        ExecutorService writerThreads = Executors.newFixedThreadPool(3);
    }
}
