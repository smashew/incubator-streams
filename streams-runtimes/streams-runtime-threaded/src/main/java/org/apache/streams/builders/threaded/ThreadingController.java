package org.apache.streams.builders.threaded;

import com.google.common.util.concurrent.*;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;

import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import org.slf4j.LoggerFactory;

public class ThreadingController {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadingController.class);

    private final ThreadPoolExecutor threadPoolExecutor;
    private final ListeningExecutorService listeningExecutorService;
    private final Condition lock = new SimpleCondition();
    private final AtomicInteger numThreads;
    private final AtomicLong lastWorked = new AtomicLong(new Date().getTime());
    private final AtomicLong numberOfObservations = new AtomicLong(0);
    private final AtomicDouble sumOfObservations = new AtomicDouble(0);
    private final AtomicInteger workingNow = new AtomicInteger(0);

    private volatile double lastCPUObservation = 0.0;

    private static final long SCALE_CHECK = 1000;
    private Double scaleThreshold = .8;
    private ThreadingControllerCPUObserver threadingControllerCPUObserver = new DefaultThreadingControllerCPUObserver();

    private static final Integer NUM_PROCESSORS = Runtime.getRuntime().availableProcessors();
    private static final Integer MAX_THREADS_ALLOWED = Math.max(100, NUM_PROCESSORS * 3);
    public static final ThreadingController INSTANCE = new ThreadingController(NUM_PROCESSORS);

    public static ThreadingController getInstance() {
        return INSTANCE;
    }

    /**
     * The current number of threads in the core pool
     * @return
     * Integer representing the number of threads in the core pool.
     */
    public Integer getNumThreads()              { return this.numThreads.get(); }

    /**
     * The time interval that is being used to determine how often to scale our threads.
     * @return
     * long in millis
     */
    public Long getScaleCheck()                 { return SCALE_CHECK; }

    /**
     * Whether or not the thread-pool is running or if all the threads are currently asleep.
     * @return
     * A boolean representing whether or not the thread-pool is running.
     */
    public boolean isRunning()                  { return this.workingNow.get() > 0; }

    /**
     * The current CPU load measured for by the observer
     * @return
     * Double representing the current CPU load
     */
    public Double getProcessCpuLoad()           { return this.threadingControllerCPUObserver.getCPUPercentUtilization(); }

    /**
     * The last CPU usage that was used to calculate whether or not the thread pool should be adjusted
     * @return
     * Double representing that observation's CPU load
     */
    public Double getLastCPUObservation()       { return this.lastCPUObservation; }

    /**
     * The number of items that are currently executing right now.
     * @return
     * An integer of the number of items executing at this very moment.
     */
    public Integer getWorkingNow()              { return workingNow.get(); }

    /**
     * The class that is being used to provide the CPU load
     * @return
     * The canonical class name that is calculating the CPU usage.
     */
    public String getProcessCpuLoadClass()      { return this.threadingControllerCPUObserver.getClass().getCanonicalName(); }

    public void setThreadingControllerCPUObserver(ThreadingControllerCPUObserver threadingControllerCPUObserver) {
        this.threadingControllerCPUObserver = threadingControllerCPUObserver;
    }

    /**
     * A double representing when the thread-pool will be adjusted if > 10% of the value.
     * Or decreased if the pool is under utilized by 10% of the value.
     * @return
     * Double representing the threshold CPU usage value (percent)
     */
    public Double getScaleThreshold() {
        return scaleThreshold;
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

    private class ThreadedCallbackWrapper implements FutureCallback<Object> {

        private final ThreadingControllerCallback callback;

        public ThreadedCallbackWrapper(ThreadingControllerCallback callback) {
            this.callback = callback;
        }

        @Override
        public void onSuccess(Object o) {
            try {
                callback.onSuccess(o);
            }
            catch(Throwable e) {
                /* */
            }
            finally {
                workingNow.decrementAndGet();
                lock.signal();
            }
        }

        @Override
        public void onFailure(Throwable t) {
            try {
                callback.onFailure(t);
            }
            catch (Throwable e) {
                /* */
            }
            finally {
                workingNow.decrementAndGet();
                lock.signal();
            }
        }


    }

    public synchronized void execute(final Runnable command, final ThreadingControllerCallback callback) {

            this.numberOfObservations.incrementAndGet();
            this.sumOfObservations.addAndGet(this.getProcessCpuLoad());

            if((new Date().getTime() > (SCALE_CHECK + this.lastWorked.get())) && this.numberOfObservations.incrementAndGet() > 3) {

                double average = this.sumOfObservations.doubleValue() / this.numberOfObservations.doubleValue();
                this.lastCPUObservation = average;

                /* re-size the shared thread-pool if we aren't under significant stress */
                int currentThreadCount = this.numThreads.get();
                int newThreadCount = this.numThreads.get();

                /* Adjust to keep the processor between 72% & 88% */
                if (average < this.scaleThreshold * .9) {
                    /* The processor isn't being worked that hard, we can add the unit here */
                    newThreadCount = Math.min(MAX_THREADS_ALLOWED, (newThreadCount + 1));
                    if(newThreadCount != currentThreadCount) {
                        LOGGER.info("+++++++ SCALING UP THREAD POOL TO {} THREADS (CPU @ {}) ++++++++", newThreadCount, average);
                    }
                } else if (average > this.scaleThreshold * 1.1) {
                    newThreadCount = Math.max((newThreadCount - 1), NUM_PROCESSORS);
                    if(newThreadCount != currentThreadCount) {
                        LOGGER.info("------- SCALING DOWN THREAD POOL TO {} THREADS (CPU @ {}) --------", newThreadCount, average);
                    }
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

            this.workingNow.incrementAndGet();

            Futures.addCallback(this.listeningExecutorService.submit(command), new ThreadedCallbackWrapper(callback));
    }
}
