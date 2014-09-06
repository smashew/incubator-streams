package org.apache.streams.builders.threaded;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

public class ThreadingController {

    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ThreadingController.class);

    private final ThreadPoolExecutor threadPoolExecutor;
    private final ListeningExecutorService listeningExecutorService;
    private final Condition lock = new SimpleCondition();
    private int numThreads;

    public ThreadingController(int numThreads) {
        this.numThreads = numThreads == 0 ? 4 : numThreads;

        this.threadPoolExecutor = new ThreadPoolExecutor(
                this.numThreads,
                this.numThreads,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(this.numThreads));

        this.threadPoolExecutor.setThreadFactory(new BasicThreadFactory.Builder()
                .namingPattern("apache-streams [threaded builder] - %d")
                .build());

        this.listeningExecutorService = MoreExecutors.listeningDecorator(this.threadPoolExecutor);
    }

    public boolean isRunning() {
        return this.threadPoolExecutor.getQueue().size() > 0;
    }

    public Condition getLock() {
        return this.lock;
    }

    public void shutDown() {
        try {
            if (!this.listeningExecutorService.isShutdown()) {
                // tell the executor to shutdown.
                this.listeningExecutorService.shutdown();
                if (!this.listeningExecutorService.awaitTermination(5, TimeUnit.MINUTES))
                    this.listeningExecutorService.shutdownNow();
            }

            LOGGER.info("Thread Handler: Shut Down");
        }
        catch(InterruptedException ioe) {
            LOGGER.warn("Error shutting down worker thread handler: {}", ioe.getMessage());
        }
    }

    public void execute(Runnable command) {
        while (this.threadPoolExecutor.getQueue().size() == this.numThreads) {
            try {
                this.lock.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        FutureCallback alertItIsDone = new FutureCallback() {
            public void onSuccess(Object o) {
                lock.signal();
            }
            public void onFailure(Throwable t) {
                lock.signal();
            }
        };

        Futures.addCallback(this.listeningExecutorService.submit(command), alertItIsDone);
    }
}
