package org.apache.streams.builders.threaded;

import com.google.common.util.concurrent.*;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;

public class ThreadingController {

    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ThreadingController.class);

    private final ThreadPoolExecutor threadPoolExecutor;
    private final ListeningExecutorService listeningExecutorService;
    private final Condition lock = new SimpleCondition();
    private final AtomicInteger numThreads;
    private final AtomicLong lastWorked = new AtomicLong(new Date().getTime());
    private final AtomicLong numberOfObservations = new AtomicLong(0);
    private final AtomicDouble sumOfObservations = new AtomicDouble(0);

    private static final long SCALE_CHECK = 1000;
    private Double scaleThreashold = .7;

    private static final Integer NUM_PROCESSORS = Runtime.getRuntime().availableProcessors();
    public static final ThreadingController INSTANCE = new ThreadingController(NUM_PROCESSORS);

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

            return value <= -1.0 ? this.scaleThreashold : (value / NUM_PROCESSORS.doubleValue());
        }
        catch(Throwable t) {
            return this.scaleThreashold;
        }
    }

    private ThreadingController() {
        this(NUM_PROCESSORS);
    }

    private ThreadingController(int numThreads) {

        this.numThreads = new AtomicInteger(numThreads);

        this.threadPoolExecutor = new ThreadPoolExecutor(
                this.numThreads.get(),
                this.numThreads.get(),
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());

        this.threadPoolExecutor.setThreadFactory(new BasicThreadFactory.Builder()
                .namingPattern("apache-streams [threaded builder] - %d")
                .build());

        this.listeningExecutorService = MoreExecutors.listeningDecorator(this.threadPoolExecutor);
    }


    private final AtomicInteger workingNow = new AtomicInteger(0);

    public synchronized void execute(final Runnable command, final ThreadingControllerCallback callback) {

        synchronized (ThreadingController.INSTANCE) {

            this.numberOfObservations.incrementAndGet();
            this.sumOfObservations.addAndGet(this.getProcessCpuLoad());

            if((new Date().getTime() > (SCALE_CHECK + this.lastWorked.get())) && this.numberOfObservations.incrementAndGet() > 5) {

                double average = this.sumOfObservations.doubleValue() / this.numberOfObservations.doubleValue();

                /* re-size the shared thread-pool if we aren't under significant stress */
                int currentThreadCount = this.numThreads.get();
                int newThreadCount = this.numThreads.get();

                /* Adjust to keep the processor between 72% & 88% */
                if (average < this.scaleThreashold * .9) {
                    /* The processor isn't being worked that hard, we can add the unit here */
                    newThreadCount = Math.min(NUM_PROCESSORS * 3, (newThreadCount + 1));
                    LOGGER.info("+++++++ SCALING UP THREAD POOL TO {} THREADS (CPU @ {}) ++++++++", newThreadCount, average);
                } else if (average > this.scaleThreashold * 1.1) {
                    newThreadCount = Math.max((newThreadCount - 1), 1);
                    LOGGER.info("------- SCALING DOWN THREAD POOL TO {} THREADS (CPU @ {}) --------", newThreadCount, average);
                }

                this.numThreads.set(newThreadCount);

                // wait
                while (this.workingNow.get() >= this.numThreads.get()) {
                    try {
                        this.lock.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                if(newThreadCount != currentThreadCount) {
                    this.threadPoolExecutor.setCorePoolSize(this.numThreads.get());
                    this.threadPoolExecutor.setMaximumPoolSize(this.numThreads.get());
                }

                this.lastWorked.set(new Date().getTime());
                this.numberOfObservations.set(0);
                this.sumOfObservations.set(0);
            }
            else {
                if(new Date().getTime() > (SCALE_CHECK + this.lastWorked.get())) {
                    // not enough observations reset the counters.
                    this.lastWorked.set(new Date().getTime());
                    this.numberOfObservations.set(0);
                    this.sumOfObservations.set(0);
                }

                while (this.workingNow.get() >= this.numThreads.get()) {
                    try {
                        this.lock.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            FutureCallback alertItIsDone = new FutureCallback() {
                public void onSuccess(Object o) {
                    workingNow.decrementAndGet();
                    lock.signal();
                    callback.onSuccess(o);
                }

                public void onFailure(Throwable t) {
                    workingNow.decrementAndGet();
                    lock.signal();
                    callback.onSuccess(t);
                }
            };

            this.workingNow.incrementAndGet();
            Futures.addCallback(this.listeningExecutorService.submit(command), alertItIsDone);
        }
    }
}
