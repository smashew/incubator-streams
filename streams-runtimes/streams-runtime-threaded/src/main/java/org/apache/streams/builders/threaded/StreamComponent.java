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
import org.joda.time.DateTime;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Stores the implementations of {@link org.apache.streams.core.StreamsOperation}, the StreamsOperations it is connected
 * to and the necessary metadata to construct a data stream.
 */
class StreamComponent {

    private static final int START = 1;
    private static final int END = 2;

    private final ThreadingController threadingController;
    private String id;
    private Set<StreamComponent> inBound;
    private Map<StreamComponent, Queue<StreamsDatum>> outBound;
    private Queue<StreamsDatum> inQueue;
    private StreamsProvider provider;
    private StreamsProcessor processor;
    private StreamsPersistWriter writer;
    private DateTime[] dateRange;
    private BigInteger sequence;
    private int numTasks = 1;
    private boolean perpetual;

    /**
     *
     * @param id
     * @param provider
     */
    public StreamComponent(ThreadingController threadingController, String id, StreamsProvider provider, boolean perpetual) {
        this.threadingController = threadingController;
        this.id = id;
        this.provider = provider;
        this.perpetual = perpetual;
        initializePrivateVariables();
    }

    /**
     *
     * @param id
     * @param provider
     * @param start
     * @param end
     */
    public StreamComponent(ThreadingController threadingController, String id, StreamsProvider provider, DateTime start, DateTime end) {
        this.threadingController = threadingController;
        this.id = id;
        this.provider = provider;
        this.dateRange = new DateTime[2];
        this.dateRange[START] = start;
        this.dateRange[END] = end;
        initializePrivateVariables();
    }


    /**
     *
     * @param id
     * @param provider
     * @param sequence
     */
    public StreamComponent(ThreadingController threadingController, String id, StreamsProvider provider, BigInteger sequence) {
        this.threadingController = threadingController;
        this.id = id;
        this.provider = provider;
        this.sequence = sequence;
    }

    /**
     *
     * @param id
     * @param processor
     * @param inQueue
     * @param numTasks
     */
    public StreamComponent(ThreadingController threadingController, String id, StreamsProcessor processor, Queue<StreamsDatum> inQueue, int numTasks) {
        this.threadingController = threadingController;
        this.id = id;
        this.processor = processor;
        this.inQueue = inQueue;
        this.numTasks = numTasks;
        initializePrivateVariables();
    }

    /**
     *
     * @param id
     * @param writer
     * @param inQueue
     * @param numTasks
     */
    public StreamComponent(ThreadingController threadingController, String id, StreamsPersistWriter writer, Queue<StreamsDatum> inQueue, int numTasks) {
        this.threadingController = threadingController;
        this.id = id;
        this.writer = writer;
        this.inQueue = inQueue;
        this.numTasks = numTasks;
        initializePrivateVariables();
    }

    private void initializePrivateVariables() {
        this.inBound = new HashSet<StreamComponent>();
        this.outBound = new HashMap<StreamComponent, Queue<StreamsDatum>>();
    }

    /**
     * Add an outbound queue for this component. The queue should be an inbound queue of a downstream component.
     * @param component the component that this supplying their inbound queue
     * @param queue the queue to to put post processed/provided datums on
     */
    public void addOutBoundQueue(StreamComponent component, Queue<StreamsDatum> queue) {
        this.outBound.put(component, queue);
    }

    /**
     * Add a component that supplies data through the inbound queue.
     * @param component that supplies data through the inbound queue
     */
    public void addInboundQueue(StreamComponent component) {
        this.inBound.add(component);
    }

    /**
     * The components that are immediately downstream of this component (aka child nodes)
     * @return Collection of child nodes of this component
     */
    public Collection<StreamComponent> getDownStreamComponents() {
        return this.outBound.keySet();
    }

    /**
     * The components that are immediately upstream of this component (aka parent nodes)
     * @return Collection of parent nodes of this component
     */
    public Collection<StreamComponent> getUpStreamComponents() {
        return this.inBound;
    }

    /**
     * The inbound queue for this component
     * @return inbound queue
     */
    public Queue<StreamsDatum> getInBoundQueue() {
        return this.inQueue;
    }

    /**
     * Creates a {@link StreamsTask} that is running a clone of this component whose
     * inbound and outbound queues are appropriately connected to the parent and child nodes.
     *
     * @return StreamsTask for this component
     */
    public StreamsTask createConnectedTask(Map<String, Object> config) {

        StreamsTask task;

        if(this.processor != null) {

            // create the task
            task = new StreamsProcessorTask(this.threadingController, this.id, config, this.processor);

            // Processor also has any number of output queues
            for(StreamComponent c : this.outBound.keySet()) {
                task.addOutputQueue(c.getId());
            }

        }
        else if(this.writer != null) {
            // create the task
            task = new StreamsPersistWriterTask(this.threadingController, this.id, config, this.writer);
        }
        else if(this.provider != null) {

            task = new StreamsProviderTask(this.threadingController, this.id, config, this.provider, StreamsProviderTask.Type.READ_CURRENT);
            for(StreamComponent c : this.outBound.keySet())
                task.addOutputQueue(c.getId());

        }
        else {
            throw new InvalidStreamException("Underlying StreamComponent was NULL.");
        }

        return task;
    }

    /**
     * The unique of this component
     * @return
     * The ID of this element
     */
    public String getId() {
        return this.id;
    }

    public int hashCode() {
        return this.id.hashCode();
    }

    public boolean equals(Object o) {
        return o instanceof StreamComponent && this.id.equals(((StreamComponent) o).id);
    }

    protected StreamsOperation getOperation() {
        if(this.processor != null) {
            return (StreamsOperation) this.processor;
        }
        else if(this.writer != null) {
            return (StreamsOperation) this.writer;
        }
        else if(this.provider != null) {
            return (StreamsOperation) this.provider;
        }
        else {
            throw new InvalidStreamException("Underlying StreamComponoent was NULL.");
        }
    }
}
