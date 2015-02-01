package org.lncwwn.assistant.exec;

import org.lncwwn.assistant.util.FileAssistant;

import java.util.concurrent.*;

/**
 * Created by victor on 1/31/15.
 */
public class Main {

    public static void main(String[] args) {

        final FileAssistant assistant = new FileAssistant();
        final String path = args[0];

        ExecutorService pool = Executors.newCachedThreadPool();

        BlockingQueue<String> readQueue = new ArrayBlockingQueue<String>(10000);
        BlockingQueue<String> parseQueue = new ArrayBlockingQueue<String>(10000);

        /**
         * reader thread
         */
        class Reader implements Runnable {
            private BlockingQueue<String> queue;
            public Reader(BlockingQueue<String> queue) {
                this.queue = queue;
            }

            @Override
            public void run() {
                assistant.reader(queue, path);
            }
        }

        /**
         * parse thread
         */
        class Parser implements Runnable {
            private BlockingQueue<String> queue1;
            private BlockingQueue<String> queue2;
            public Parser(BlockingQueue<String> queue1, BlockingQueue<String> queue2) {
                this.queue1 = queue1;
                this.queue2 = queue2;
            }
            @Override
            public void run() {
                assistant.parse(queue1, queue2);
            }
        }

        /**
         * writer thread
         */
        class Writer implements Runnable {
            private BlockingQueue<String> queue;
            public Writer(BlockingQueue<String> queue) {
                this.queue = queue;
            }
            @Override
            public void run() {
                assistant.write(queue, path);
            }
        }

        pool.submit(new Reader(readQueue));
        pool.submit(new Parser(readQueue, parseQueue));
        pool.submit(new Writer(parseQueue));

    }

}
