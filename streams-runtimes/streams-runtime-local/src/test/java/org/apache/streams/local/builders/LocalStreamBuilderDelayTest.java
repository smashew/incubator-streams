package org.apache.streams.local.builders;

import org.apache.streams.core.StreamBuilder;
import org.apache.streams.local.test.processors.PassThroughStaticCounterProcessor;
import org.apache.streams.local.test.processors.SimpleProcessorCounter;
import org.apache.streams.local.test.providers.NumericMessageProvider;
import org.apache.streams.local.test.providers.NumericMessageProviderDelayed;
import org.apache.streams.local.test.writer.DatumCounterWriter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * These tests ensure that StreamsBuilder works
 */
public class LocalStreamBuilderDelayTest {

    @Test
    public void delayedWriterTest() {

        int numDatums = 5;
        StreamBuilder builder = new LocalStreamBuilder();
        DatumCounterWriter writer = new DatumCounterWriter(500); // give the DatumCounter a 500ms delay
        SimpleProcessorCounter proc1 = new SimpleProcessorCounter();
        SimpleProcessorCounter proc2 = new SimpleProcessorCounter();

        builder.newReadCurrentStream("prov1", new NumericMessageProvider(numDatums))
                .addStreamsProcessor("proc1", proc1, 1, "prov1")
                .addStreamsProcessor("proc2", proc2, 1, "prov1")
                .addStreamsPersistWriter("w1", writer, 1, "proc1", "proc2");

        builder.start();

        assertEquals("Number in should equal number out", numDatums, proc1.getMessageCount());
        assertEquals("Number in should equal number out", numDatums, proc2.getMessageCount());
        assertEquals("Number in should equal number out", numDatums * 2, writer.getDatumsCounted());
    }

    @Test
    public void delayedProcessorTest() {
        int numDatums = 5;
        StreamBuilder builder = new LocalStreamBuilder();
        DatumCounterWriter writer = new DatumCounterWriter();
        SimpleProcessorCounter proc1 = new SimpleProcessorCounter(500);
        SimpleProcessorCounter proc2 = new SimpleProcessorCounter(250);

        builder.newReadCurrentStream("prov1", new NumericMessageProvider(numDatums))
                .addStreamsProcessor("proc1", proc1, 1, "prov1")
                .addStreamsProcessor("proc2", proc2, 1, "prov1")
                .addStreamsPersistWriter("w1", writer, 1, "proc1", "proc2");

        builder.start();

        assertEquals("Number in should equal number out", numDatums, proc1.getMessageCount());
        assertEquals("Number in should equal number out", numDatums, proc2.getMessageCount());
        assertEquals("Number in should equal number out", numDatums * 2, writer.getDatumsCounted());
    }


    @Test
    public void delayedProviderTest()  {
        int numDatums = 10;
        StreamBuilder builder = new LocalStreamBuilder();

        NumericMessageProviderDelayed provider = new NumericMessageProviderDelayed(numDatums, 100);
        PassThroughStaticCounterProcessor processor = new PassThroughStaticCounterProcessor(250);
        DatumCounterWriter writer = new DatumCounterWriter(125);
        builder.newReadCurrentStream("sp1", provider)
                .addStreamsProcessor("proc1", processor, 1, "sp1")
                .addStreamsPersistWriter("writer1", writer, 1, "proc1");
        builder.start();
        assertEquals("Should have same number", numDatums, writer.getDatumsCounted());
    }

    @Test
    public void everythingDelayedTest()  {
        int numDatums = 100;
        StreamBuilder builder = new LocalStreamBuilder();
        PassThroughStaticCounterProcessor processor = new PassThroughStaticCounterProcessor();
        DatumCounterWriter writer = new DatumCounterWriter();
        builder.newReadCurrentStream("sp1", new NumericMessageProviderDelayed(numDatums, 100))
                .addStreamsProcessor("proc1", processor, 1, "sp1")
                .addStreamsPersistWriter("writer1", writer, 1, "proc1");
        builder.start();
        assertEquals("Should have same number", numDatums, writer.getDatumsCounted());
    }


}