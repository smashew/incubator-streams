package org.apache.streams.builders.threaded;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;

public class ThreadingController {

    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ThreadingController.class);

    private final ThreadPoolExecutor threadPoolExecutor;
    private final ListeningExecutorService listeningExecutorService;
    private final Condition lock = new SimpleCondition();

    private final AtomicInteger numThreads;
    private double scaleThreashold = .8;

    private static final Integer NUM_PROCESSORS = Runtime.getRuntime().availableProcessors();

    private static final ThreadingController INSTANCE = new ThreadingController(Runtime.getRuntime().availableProcessors());

    public static ThreadingController getInstance() {
        return INSTANCE;
    }

    public Integer getNumThreads() {
        return this.numThreads.get();
    }


    private double getProcessCpuLoad()  {

        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = ObjectName.getInstance("java.lang:type=OperatingSystem");
            AttributeList list = mbs.getAttributes(name, new String[]{"ProcessCpuLoad"});

            if (list.isEmpty()) return Double.NaN;

            Attribute att = (Attribute) list.get(0);
            Double value = (Double) att.getValue();

            if (value == -1.0) return Double.NaN;  // usually takes a couple of seconds before we get real values

            return ((int) (value * 1000) / 10.0);        // returns a percentage value with 1 decimal point precision
        }
        catch(Throwable t) {
            return this.scaleThreashold;
        }
    }

    private ThreadingController(int numThreads) {

        this.numThreads = new AtomicInteger(numThreads);

        this.threadPoolExecutor = new ThreadPoolExecutor(
                this.numThreads.get(),
                this.numThreads.get(),
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(this.numThreads.get()));

        this.threadPoolExecutor.setThreadFactory(new BasicThreadFactory.Builder()
                .namingPattern("apache-streams [threaded builder] - %d")
                .build());

        this.listeningExecutorService = MoreExecutors.listeningDecorator(this.threadPoolExecutor);
    }

    public void execute(final Runnable command, final ThreadingControllerCallback callback) {

        /* re-size the shared thread-pool if we aren't under significant stress */
        if(this.getProcessCpuLoad() > this.scaleThreashold) {
            this.numThreads.set(Math.max(NUM_PROCESSORS * 3, this.numThreads.incrementAndGet()));
        }
        else {
            this.numThreads.set(Math.max(this.numThreads.decrementAndGet(), 1));
        }

        this.threadPoolExecutor.setCorePoolSize(this.numThreads.get());

        while (this.threadPoolExecutor.getQueue().size() == this.numThreads.get()) {
            try {
                this.lock.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        FutureCallback alertItIsDone = new FutureCallback() {
            public void onSuccess(Object o) {
                lock.signal();
                callback.onSuccess(o);
            }
            public void onFailure(Throwable t) {
                lock.signal();
                callback.onSuccess(t);
            }
        };

        Futures.addCallback(this.listeningExecutorService.submit(command), alertItIsDone);
    }
}
