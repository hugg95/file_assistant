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

        ExecutorService readerThreads = Executors.newFixedThreadPool(2);
        ExecutorService parseThreads = Executors.newFixedThreadPool(3);
        ExecutorService writerThreads = Executors.newFixedThreadPool(1);

        BlockingQueue<String> readQueue = new ArrayBlockingQueue<String>(10000);
        BlockingQueue<String> parseQueue = new ArrayBlockingQueue<String>(10000);

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

        class Parser implements Runnable {
            private BlockingQueue<String> queue1;
            private BlockingQueue<String> queue2;
            public Parser(BlockingQueue<String> queue1, BlockingQueue<String> queue2) {
                this.queue1 = queue1;
                this.queue2 = queue2;
            }
            @Override
            public void run() {
                assistant.analyse(queue1, queue2);
            }
        }

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

        readerThreads.execute(new Reader(readQueue));
        parseThreads.execute(new Parser(readQueue, parseQueue));
        writerThreads.execute(new Writer(parseQueue));

    }

}
