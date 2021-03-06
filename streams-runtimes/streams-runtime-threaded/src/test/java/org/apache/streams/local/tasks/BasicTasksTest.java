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
package org.apache.streams.local.tasks;

import org.apache.streams.builders.threaded.*;
import org.apache.streams.core.StreamsDatum;
import org.apache.streams.local.test.processors.PassThroughStaticCounterProcessor;
import org.apache.streams.local.test.providers.NumericMessageProvider;
import org.apache.streams.local.test.writer.DatumCounterWriter;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class BasicTasksTest {

    /**
     * This test has been ignored, literally, it makes absolutely no sense.
     */
    @Ignore
    @Test
    public void testProviderTask() {
        /*
        int numMessages = 100;

        NumericMessageProvider provider = new NumericMessageProvider(numMessages);
        StreamsProviderTask task = new StreamsProviderTask(new ThreadingController(1), "id", new HashMap<String, BaseStreamsTask>(), provider, false);
        Queue<StreamsDatum> outQueue = new ConcurrentLinkedQueue<StreamsDatum>();
        task.addOutputQueue("out", outQueue);
        Queue<StreamsDatum> inQueue = createInputQueue(numMessages);
        Exception exp = null;
        try {
            task.addInputQueue("in", inQueue);
        } catch (UnsupportedOperationException uoe) {
            exp = uoe;
        }
        assertNotNull(exp);
        ExecutorService service = Executors.newFixedThreadPool(1);
        service.submit(task);
        int attempts = 0;
        while (outQueue.size() != numMessages) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                //Ignore
            }
            ++attempts;
            if (attempts == 10) {
                fail("Provider task failed to output " + numMessages + " in a timely fashion.");
            }
        }
        service.shutdown();
        try {
            if (!service.awaitTermination(5, TimeUnit.SECONDS)) {
                service.shutdownNow();
                fail("Service did not terminate.");
            }
            assertTrue("Task should have completed running in aloted time.", service.isTerminated());
        } catch (InterruptedException e) {
            fail("Test Interupted.");
        }
        */
    }

    @Ignore
    @Test
    public void testProcessorTask() {
        /*
        int numMessages = 100;

        PassThroughStaticCounterProcessor processor = new PassThroughStaticCounterProcessor();

        StreamsProcessorTask task = new StreamsProcessorTask("id", new HashMap<String, BaseStreamsTask>(), processor, new ThreadingController(1));
        Queue<StreamsDatum> outQueue = new ConcurrentLinkedQueue<StreamsDatum>();
        Queue<StreamsDatum> inQueue = createInputQueue(numMessages);
        task.addOutputQueue("out", outQueue);
        task.addInputQueue("in", inQueue);
        assertEquals(numMessages, task.getInputQueues().get(0).size());
        ExecutorService service = Executors.newFixedThreadPool(1);
        service.submit(task);
        int attempts = 0;
        while (inQueue.size() != 0 && outQueue.size() != numMessages) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                //Ignore
            }
            ++attempts;
            if (attempts == 10) {
                fail("Processor task failed to output " + numMessages + " in a timely fashion.");
            }
        }
        task.stopTask();
        assertEquals(numMessages, processor.getMessageCount());
        service.shutdown();
        try {
            if (!service.awaitTermination(5, TimeUnit.SECONDS)) {
                service.shutdownNow();
                fail("Service did not terminate.");
            }
            assertTrue("Task should have completed running in aloted time.", service.isTerminated());
        } catch (InterruptedException e) {
            fail("Test Interupted.");
        }
        */
    }

    @Ignore
    @Test
    public void testWriterTask() {
        /*
        int numMessages = 100;

        DatumCounterWriter writer = new DatumCounterWriter();
        StreamsPersistWriterTask task = new StreamsPersistWriterTask("id", new HashMap<String, BaseStreamsTask>(), writer, new ThreadingController(1));
        Queue<StreamsDatum> outQueue = new ConcurrentLinkedQueue<StreamsDatum>();
        Queue<StreamsDatum> inQueue = createInputQueue(numMessages);

        Exception exp = null;
        try {
            task.addOutputQueue("out", outQueue);
        } catch (UnsupportedOperationException uoe) {
            exp = uoe;
        }
        assertNotNull(exp);
        task.addInputQueue("in", inQueue);
        assertEquals(numMessages, task.getInputQueues().get(0).size());
        ExecutorService service = Executors.newFixedThreadPool(1);
        service.submit(task);
        int attempts = 0;
        while (inQueue.size() != 0) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                //Ignore
            }
            ++attempts;
            if (attempts == 10) {
                fail("Processor task failed to output " + numMessages + " in a timely fashion.");
            }
        }
        task.stopTask();
        assertEquals(numMessages, writer.getDatumsCounted());
        service.shutdown();
        try {
            if (!service.awaitTermination(5, TimeUnit.SECONDS)) {
                service.shutdownNow();
                fail("Service did not terminate.");
            }
            assertTrue("Task should have completed running in aloted time.", service.isTerminated());
        } catch (InterruptedException e) {
            fail("Test Interupted.");
        }
        */
    }

    @Ignore
    @Test
    public void testBranching() {
        /*
        int numMessages = 100;

        PassThroughStaticCounterProcessor processor = new PassThroughStaticCounterProcessor();
        StreamsProcessorTask task = new StreamsProcessorTask("id", new HashMap<String, BaseStreamsTask>(), processor, new ThreadingController(1));
        Queue<StreamsDatum> outQueue1 = new ConcurrentLinkedQueue<StreamsDatum>();
        Queue<StreamsDatum> outQueue2 = new ConcurrentLinkedQueue<StreamsDatum>();
        Queue<StreamsDatum> inQueue = createInputQueue(numMessages);
        task.addOutputQueue("o1", outQueue1);
        task.addOutputQueue("o2", outQueue2);
        task.addInputQueue("in", inQueue);
        assertEquals(numMessages, task.getInputQueues().get(0).size());
        ExecutorService service = Executors.newFixedThreadPool(1);
        service.submit(task);
        int attempts = 0;
        while (inQueue.size() != 0) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                //Ignore
            }
            ++attempts;
            if (attempts == 10) {
                assertEquals("Processor task failed to output " + (numMessages) + " in a timely fashion.", 0, inQueue.size());
            }
        }
        task.stopTask();

        service.shutdown();
        try {
            if (!service.awaitTermination(5, TimeUnit.MINUTES)) {
                service.shutdownNow();
                fail("Service did not terminate.");
            }
            assertTrue("Task should have completed running in aloted time.", service.isTerminated());
        } catch (InterruptedException e) {
            fail("Test Interupted.");
        }
        assertEquals(numMessages, processor.getMessageCount());
        assertEquals(numMessages, outQueue1.size());
        assertEquals(numMessages, outQueue2.size());
        */
    }

    @Ignore
    @Test
    public void testBranchingSerialization() {
        /*
        int numMessages = 1;
        PassThroughStaticCounterProcessor processor = new PassThroughStaticCounterProcessor();
        StreamsProcessorTask task = new StreamsProcessorTask("id", new HashMap<String, BaseStreamsTask>(), processor, new ThreadingController(1));
        Queue<StreamsDatum> outQueue1 = new ConcurrentLinkedQueue<StreamsDatum>();
        Queue<StreamsDatum> outQueue2 = new ConcurrentLinkedQueue<StreamsDatum>();
        Queue<StreamsDatum> inQueue = createInputQueue(numMessages);
        task.addOutputQueue("o1", outQueue1);
        task.addOutputQueue("o2", outQueue2);
        task.addInputQueue("in", inQueue);
        ExecutorService service = Executors.newFixedThreadPool(1);
        service.submit(task);
        int attempts = 0;
        while (inQueue.size() != 0) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                //Ignore
            }
            ++attempts;
            if (attempts == 10) {
                assertEquals("Processor task failed to output " + (numMessages) + " in a timely fashion.", 0, inQueue.size());
            }
        }
        task.stopTask();

        service.shutdown();
        try {
            if (!service.awaitTermination(5, TimeUnit.SECONDS)) {
                service.shutdownNow();
                fail("Service did not terminate.");
            }
            assertTrue("Task should have completed running in aloted time.", service.isTerminated());
        } catch (InterruptedException e) {
            fail("Test Interupted.");
        }
        assertEquals(numMessages, processor.getMessageCount());
        assertEquals(numMessages, outQueue1.size());
        assertEquals(numMessages, outQueue2.size());
        StreamsDatum datum1 = outQueue1.poll();
        StreamsDatum datum2 = outQueue2.poll();
        assertNotNull(datum1);
        assertEquals(datum1, datum2);
        datum1.setDocument("a");
        assertNotEquals(datum1, datum2);
        */
    }

    private Queue<StreamsDatum> createInputQueue(int numDatums) {
        Queue<StreamsDatum> queue = new ConcurrentLinkedQueue<StreamsDatum>();
        for (int i = 0; i < numDatums; ++i) {
            queue.add(new StreamsDatum(i));
        }
        return queue;
    }


}
