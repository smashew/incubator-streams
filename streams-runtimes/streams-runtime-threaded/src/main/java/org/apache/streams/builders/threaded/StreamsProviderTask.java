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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;

public class StreamsProviderTask extends BaseStreamsTask implements Runnable {

    private final static Logger LOGGER = LoggerFactory.getLogger(StreamsProviderTask.class);

    private StreamsProvider provider;
    private final Type type;
    private final AtomicBoolean keepRunning = new AtomicBoolean(true);
    private final Condition lock = new SimpleCondition();
    private final ThreadingController threadingController;

    public static enum Type {
        PERPETUAL,
        READ_CURRENT,
        READ_NEW,
        READ_RANGE
    }

    private static final int START = 0;
    private static final int END = 1;

    private static final int TIMEOUT = 100000000;
    private static final int DEFAULT_SLEEP = 200;

    public StreamsProviderTask(String id, Map<String, Object> config, StreamsProvider provider, Type type, ThreadingController threadingController) {
        super(id, config);
        this.provider = provider;
        this.type = type;
        this.threadingController = threadingController;
    }

    public boolean isRunning() {
        return this.keepRunning.get();
    }

    public Condition getLock() {
        return this.lock;
    }

    @Override
    public void run() {

        try {
            // TODO allow for configuration objects
            this.provider.prepare(this.config);
            StreamsResultSet resultSet = null;
            switch (this.type) {
                case PERPETUAL:
                    provider.startStream();
                    int zeros = 0;
                    while (this.keepRunning.get()) {
                        resultSet = provider.readCurrent();
                        if (resultSet.size() == 0)
                            zeros++;
                        else {
                            zeros = 0;
                        }
                        flushResults(resultSet);
                        // the way this works needs to change...
                        if (zeros > TIMEOUT)
                            this.keepRunning.set(false);
                        safeQuickRest(10);
                    }
                    break;
                case READ_CURRENT:
                    resultSet = this.provider.readCurrent();
                    break;
                case READ_NEW:
                case READ_RANGE:
                default:
                    throw new RuntimeException("Type has not been added to StreamsProviderTask.");
            }

            if(resultSet != null && resultSet.getQueue() != null) {

                while (resultSet.isRunning() || resultSet.getQueue().size() > 0) {
                    // Is there anything to do?
                    if(resultSet.getQueue().isEmpty()) {
                        safeQuickRest(5);           // then rest
                    } else {
                        flushResults(resultSet);    // then work
                    }
                }
            }
        } catch (Throwable e) {
            LOGGER.error("There was an unknown error while attempting to read from the provider. This provider cannot continue with this exception.");
            LOGGER.error("The stream will continue running, but not with this provider.");
            LOGGER.error("Exception: {}", e);
        } finally {
            this.provider.cleanUp();
            this.keepRunning.set(false);
            this.lock.signalAll();
        }
    }

    public void flushResults(StreamsResultSet streamsResultSet) {
        try {
            StreamsDatum datum;
            while (!streamsResultSet.getQueue().isEmpty() && (datum = streamsResultSet.getQueue().poll()) != null) {
                /**
                 * This is meant to be a hard exit from the system. If we are running
                 * and this flag gets set to false, we are to exit immediately and
                 * abandon anything that is in this queue. The remaining processors
                 * will shutdown gracefully once they have depleted their queue
                 */
                if (!this.keepRunning.get())
                    break;

                workMe(datum);
            }
        }
        catch(Throwable e) {
            LOGGER.warn("Unknown problem reading the queue, no datums affected: {}", e.getMessage());
        }
    }

    @Override
    public void cleanup() {
        this.provider.cleanUp();
    }

    private void workMe(final StreamsDatum datum) {
        this.threadingController.execute(new Runnable() {
            @Override
            public void run() {
                sendToChildren(datum);
            }
        });
    }

    @Override
    protected Collection<StreamsDatum> processInternal(StreamsDatum datum) {
        return null;
    }

    protected void safeQuickRest(int waitTime) {

        // The queue is empty, we might as well sleep.
        Thread.yield();
        try {
            Thread.sleep(waitTime);
        } catch (InterruptedException ie) {
            // No Operation
        }
    }

}
