/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.streams.builders.threaded;

import org.apache.streams.core.*;
import org.apache.streams.util.SerializationUtil;
import org.joda.time.DateTime;
import org.slf4j.Logger;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * {@link ThreadedStreamBuilder} implementation to run a data processing stream in a single
 * JVM across many threads.  Depending on your data stream, the JVM heap may need to be set to a high value. Default
 * implementation uses unbound {@link java.util.concurrent.ConcurrentLinkedQueue} to connect stream components.
 */
public class ThreadedStreamBuilder implements StreamBuilder {

    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ThreadedStreamBuilder.class);

    private final List<StreamsGraphElement> graphElements = new ArrayList<StreamsGraphElement>();

    public static final String TIMEOUT_KEY = "TIMEOUT";
    private final Queue<StreamsDatum> queue;
    private final Map<String, StreamComponent> providers;
    private final Map<String, StreamComponent> components;
    private final Map<String, Object> streamConfig;
    private final Map<String, StreamsTask> tasks = new LinkedHashMap<String, StreamsTask>();
    private final Collection<StreamBuilderEventHandler> eventHandlers = new ArrayList<StreamBuilderEventHandler>();

    private final ThreadingController threadingController;

    private Thread shutDownHandler;

    /**
     *
     */
    public ThreadedStreamBuilder() {
        this(new ArrayBlockingQueue<StreamsDatum>(50), null);
    }

    /**
     * @param queue
     */
    public ThreadedStreamBuilder(Queue<StreamsDatum> queue) {
        this(queue, null);
    }

    public List<StreamsGraphElement> getGraphElements() {
        if(this.graphElements.size() == 0)
            buildGraphElements();
        return this.graphElements;
    }


    private void buildGraphElements() {
        this.graphElements.clear();
        for(StreamsTask p : this.tasks.values()) {
            if(p instanceof StreamsProviderTask)
                appendAndFollow(p, this.graphElements);
        }
    }

    private void appendAndFollow(StreamsTask t, List<StreamsGraphElement> elems) {
        for(StreamsTask c : t.getChildren()) {

            String type;
            if(t.getClass().equals(StreamsProviderTask.class))
                type = "provider";
            else if(t.getClass().equals(StreamsProcessorTask.class))
                type = "processor";
            else if(t.getClass().equals(StreamsPersistWriterTask.class))
                type = "writer";
            else
                type = "unknown";

            elems.add(new StreamsGraphElement(t.getId(), c.getId(), type, 0));
            appendAndFollow(c, elems);
        }
    }


    /**
     * @param streamConfig
     */
    public ThreadedStreamBuilder(Map<String, Object> streamConfig) {
        this(new ArrayBlockingQueue<StreamsDatum>(50), streamConfig);
    }

    public ThreadedStreamBuilder(Queue<StreamsDatum> queue, Map<String, Object> streamConfig) {

        this.queue = queue;
        this.providers = new LinkedHashMap<String, StreamComponent>();
        this.components = new LinkedHashMap<String, StreamComponent>();
        this.streamConfig = streamConfig;

        this.threadingController = ThreadingController.getInstance();
    }

    @Override
    public ThreadedStreamBuilder newPerpetualStream(String id, StreamsProvider provider) {
        validateId(id);
        this.providers.put(id, new StreamComponent(id, provider, true));
        return this;
    }

    @Override
    public ThreadedStreamBuilder newReadCurrentStream(String id, StreamsProvider provider) {
        validateId(id);
        this.providers.put(id, new StreamComponent(id, provider, false));
        return this;
    }

    @Override
    public ThreadedStreamBuilder newReadNewStream(String id, StreamsProvider provider, BigInteger sequence) {
        validateId(id);
        this.providers.put(id, new StreamComponent(id, provider, sequence));
        return this;
    }

    @Override
    public ThreadedStreamBuilder newReadRangeStream(String id, StreamsProvider provider, DateTime start, DateTime end) {
        validateId(id);
        this.providers.put(id, new StreamComponent(id, provider, start, end));
        return this;
    }

    public ThreadedStreamBuilder addStreamsProcessor(String id, StreamsProcessor processor, String... inBoundIds) {
        addStreamsProcessor(id, processor, 1, inBoundIds);
        return this;
    }

    @Override
    public ThreadedStreamBuilder addStreamsProcessor(String id, StreamsProcessor processor, int numTasks, String... inBoundIds) {
        validateId(id);
        StreamComponent comp = new StreamComponent(id, processor, cloneQueue(), numTasks);
        this.components.put(id, comp);
        connectToOtherComponents(inBoundIds, comp);
        return this;
    }

    @Override
    public ThreadedStreamBuilder addStreamsPersistWriter(String id, StreamsPersistWriter writer, int numTasks, String... inBoundIds) {
        validateId(id);
        StreamComponent comp = new StreamComponent(id, writer, cloneQueue(), numTasks);
        this.components.put(id, comp);
        connectToOtherComponents(inBoundIds, comp);
        return this;
    }

    private ExecutorService executor;

    public ThreadedStreamBuilder addEventHandler(StreamBuilderEventHandler eventHandler) {
        this.eventHandlers.add(eventHandler);
        return this;
    }

    public ThreadedStreamBuilder removeEventHandler(StreamBuilderEventHandler eventHandler) {
        if(this.eventHandlers.contains(eventHandler))
            this.eventHandlers.remove(eventHandler);
        return this;
    }

    public final Map<String, StatusCounts> getUpdateCounts() {
        final Map<String, StatusCounts> updateMap = new HashMap<String, StatusCounts>();

        for (final String k : tasks.keySet())
            updateMap.put(k, tasks.get(k).getCurrentStatus());
        return updateMap;
    }

    /**
     * Runs the data stream in the this JVM and blocks till completion.
     */
    @Override
    public void start() {

        final Timer timer = new Timer(true);

        if (this.shutDownHandler != null) {
            String message = "The stream builder has already been started and has not been successfully shutdown. Nothing will execute.";
            LOGGER.warn(message);
            throw new RuntimeException(message);
        }

        // Notice, we are making a reference to 'self' we need to remove this handler
        // once we are completed ot ensure we don't hold onto this object reference
        final ThreadedStreamBuilder self = this;
        this.shutDownHandler = new Thread() {
            @Override
            public void run() {
                LOGGER.debug("Shutdown hook received.  Beginning shutdown");
                self.stop();
            }
        };

        Runtime.getRuntime().addShutdownHook(shutDownHandler);

        this.tasks.clear();
        createTasks();

        int numProviders = 0;
        for(StreamsTask task : this.tasks.values())
            if(task instanceof Runnable)
                numProviders++;

        this.executor = Executors.newFixedThreadPool(numProviders);

        try {

            // if anyone would like to listen in to progress events
            // let them do that
            TimerTask updateTask = new TimerTask() {
                public void run() {

                    final Map<String, StatusCounts> updateMap = getUpdateCounts();

                    for(String k : updateMap.keySet())
                        for(StreamsGraphElement g : getGraphElements())
                            if(g.getTarget().equals(k))
                                g.setValue((int)updateMap.get(k).getWorking());


                    if (eventHandlers.size() > 0) {
                        for (final StreamBuilderEventHandler eventHandler : eventHandlers) {
                            try {
                                try {
                                    eventHandler.update(updateMap, getGraphElements());
                                } catch(Throwable e) {
                                    /* */
                                }
                            }
                            catch(Throwable e) {
                                /* No Operation */
                            }
                        }
                    }
                }
            };

            timer.schedule(updateTask, 0, 1500);

            // Starting all the tasks
            for(StreamsTask task : this.tasks.values())
                if(task instanceof Runnable)
                    this.executor.execute((Runnable)task);

            for(StreamsTask t : this.tasks.values())
                    t.prepare(this.streamConfig);


            // Starting all the tasks
            for(StreamsTask task : this.tasks.values())
                if(task instanceof Runnable)
                    this.executor.execute((Runnable)task);

            Condition condition = null;
            while((condition = getOffendingLock()) != null) {
                condition.await();
            }

            this.executor.shutdown();
            this.executor.awaitTermination(30, TimeUnit.MINUTES);

            for(StreamsTask t : this.tasks.values())
                t.cleanup();

            for(final String k : tasks.keySet()) {
                final StatusCounts counts = tasks.get(k).getCurrentStatus();
                LOGGER.debug("Finishing: {} - Working[{}] Success[{}] Failed[{}] TimeSpent[{}]", k,
                        counts.getWorking(), counts.getSuccess(), counts.getFailed(), counts.getAverageTimeSpent());
            }


        } catch (Throwable e) {
            // No Operation
            try {
                shutdown();
            }
            catch (Throwable omgE) {
                LOGGER.error("Unexpected Error: {}", omgE);
            }
        } finally {
            if (!Runtime.getRuntime().removeShutdownHook(this.shutDownHandler))
                LOGGER.warn("We should have removed the shutdown handler...");

            this.shutDownHandler = null;

            // Kill the timer
            timer.cancel();
        }
    }

    private Condition getOffendingLock() {
        for(StreamsTask t : this.tasks.values()) {
            if(t instanceof StreamsProviderTask)
                if(((StreamsProviderTask)t).isRunning())
                    return ((StreamsProviderTask)t).getLock();
        }
        return null;
    }

    private void shutdownExecutor() {
        // make sure that
        try {
            if (!this.executor.isShutdown()) {
                // tell the executor to shutdown.
                this.executor.shutdown();

                if (!this.executor.awaitTermination(5, TimeUnit.MINUTES))
                    this.executor.shutdownNow();
            }
        } catch (InterruptedException ie) {
            this.executor.shutdownNow();
            throw new RuntimeException(ie);
        }
    }

    protected void shutdown() throws InterruptedException {
        LOGGER.debug("Shutting down...");
    }

    protected void createTasks() {
        for (StreamComponent prov : this.providers.values())
            this.tasks.put(prov.getId(), prov.createConnectedTask(this.streamConfig, this.threadingController));

        for (StreamComponent comp : this.components.values())
            this.tasks.put(comp.getId(), comp.createConnectedTask(this.streamConfig, this.threadingController));

        for(StreamsTask t : this.tasks.values())
            t.initialize(this.tasks);
    }

    public void stop() {
        try {
            shutdown();
        } catch (Exception e) {
            LOGGER.warn("Forcing Shutdown: There was an error stopping: {}", e.getMessage());
        }
    }

    private void connectToOtherComponents(String[] connectToIds, StreamComponent toBeConnected) {
        for (String id : connectToIds) {
            StreamComponent upStream;
            if (this.providers.containsKey(id)) {
                upStream = this.providers.get(id);
            } else if (this.components.containsKey(id)) {
                upStream = this.components.get(id);
            } else {
                throw new InvalidStreamException("Cannot connect to id, " + id + ", because id does not exist.");
            }
            upStream.addOutBoundQueue(toBeConnected, toBeConnected.getInBoundQueue());
            toBeConnected.addInboundQueue(upStream);
        }
    }

    private void validateId(String id) {
        if (this.providers.containsKey(id) || this.components.containsKey(id)) {
            throw new InvalidStreamException("Duplicate id. " + id + " is already assigned to another component");
        }
    }

    @SuppressWarnings("unchecked")
    private Queue<StreamsDatum> cloneQueue() {
        Object toReturn = SerializationUtil.cloneBySerialization(this.queue);
        if(toReturn instanceof Queue)
            return (Queue<StreamsDatum>)toReturn;
        else
            throw new RuntimeException("Unable to clone the queue");
    }

    protected int getTimeout() {
        return streamConfig != null && streamConfig.containsKey(TIMEOUT_KEY) ? (Integer) streamConfig.get(TIMEOUT_KEY) : 3000;
    }

}
