package org.apache.streams.urls;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.streams.core.StreamsDatum;
import org.apache.streams.jackson.StreamsJacksonMapper;
import org.apache.streams.jackson.StreamsJacksonModule;
import org.apache.streams.pojo.json.Activity;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class TestLinkUnwinderProcessor {

    private static String activityString;

    @Test
    public void testLinkUnwinderBadDomain() {
        LinkResolver resolver = new LinkResolver("http://nope@#$%");
        resolver.run();
        LinkDetails details = resolver.getLinkDetails();
        assertEquals("Should be 404", details.getLinkStatus(), LinkDetails.LinkStatus.MALFORMED_URL);
    }

    @Test
    public void testLinkResolverSerializability() {
        LinkResolver resolver = new LinkResolver("http://bit.ly/1cX5Rh4");
        LinkResolver clone = SerializationUtils.clone(resolver);
    }

    @Test
    public void test404Link() {
        LinkResolver resolver = new LinkResolver("http://www.kneesupmotherbrown.me/2013/05/26/well-its-fair-to-say-may-has-been-a-crappy-month");
        resolver.run();
        LinkDetails details = resolver.getLinkDetails();
        assertEquals("Should be 404", details.getLinkStatus(), LinkDetails.LinkStatus.NOT_FOUND);
    }

    @Test
    public void testLinkResolverProcessorSerializability() {
        LinkResolverProcessor processor = new LinkResolverProcessor();
        LinkResolverProcessor clone = SerializationUtils.clone(processor);
    }

    @Test
    public void testActivityLinkUnwinderProcessorBitly() throws Exception{
        testActivityUnwinderHelper(Lists.newArrayList("http://bit.ly/1cX5Rh4"), Lists.newArrayList("http://www.wcgworld.com/"));
        testStringActivityUnwinderHelper(Lists.newArrayList("http://bit.ly/1cX5Rh4"), Lists.newArrayList("http://www.wcgworld.com/"));
    }

    @Test
    public void testActivityLinkUnwinderProcessorTdotCo() throws Exception{
        testActivityUnwinderHelper(Lists.newArrayList("http://t.co/XDkGt0vJaF"), Lists.newArrayList("https://instagram.com/p/ibkJDWK97z/"));
        testStringActivityUnwinderHelper(Lists.newArrayList("http://t.co/XDkGt0vJaF"), Lists.newArrayList("https://instagram.com/p/ibkJDWK97z/"));
    }

    @Test
    public void testActivityLinkUnwinderProcessorGoogle() throws Exception{
        testActivityUnwinderHelper(Lists.newArrayList("http://goo.gl/wSrHDA"), Lists.newArrayList("http://www.wcgworld.com/"));
        testStringActivityUnwinderHelper(Lists.newArrayList("http://goo.gl/wSrHDA"), Lists.newArrayList("http://www.wcgworld.com/"));
    }

    @Test
    public void testActivityLinkUnwinderProcessorOwly() throws Exception{
        testActivityUnwinderHelper(Lists.newArrayList("http://ow.ly/u4Kte"), Lists.newArrayList("http://www.wcgworld.com/"));
        testStringActivityUnwinderHelper(Lists.newArrayList("http://ow.ly/u4Kte"), Lists.newArrayList("http://www.wcgworld.com/"));
    }

    @Test
    public void testDecember2014Owly() throws Exception {
        testActivityUnwinderHelper(Lists.newArrayList("http://ow.ly/FGboo"), Lists.newArrayList("http://www.smashew.com/2013/08/29/api-part-2/"));
        testStringActivityUnwinderHelper(Lists.newArrayList("http://ow.ly/FGboo"), Lists.newArrayList("http://www.smashew.com/2013/08/29/api-part-2/"));
    }

    @Test
    public void testActivityLinkUnwinderProcessorGoDaddy() throws Exception{
        testActivityUnwinderHelper(Lists.newArrayList("http://x.co/3yapt"), Lists.newArrayList("http://www.wcgworld.com/"));
        testStringActivityUnwinderHelper(Lists.newArrayList("http://x.co/3yapt"), Lists.newArrayList("http://www.wcgworld.com/"));
    }

    @Test
    public void testActivityLinkUnwinderProcessorMulti() throws Exception{
        testActivityUnwinderHelper(Lists.newArrayList("http://x.co/3yapt", "http://ow.ly/u4Kte", "http://goo.gl/wSrHDA"), Lists.newArrayList("http://www.wcgworld.com/", "http://www.wcgworld.com/", "http://www.wcgworld.com/"));
        testStringActivityUnwinderHelper(Lists.newArrayList("http://x.co/3yapt", "http://ow.ly/u4Kte", "http://goo.gl/wSrHDA"), Lists.newArrayList("http://www.wcgworld.com/", "http://www.wcgworld.com/", "http://www.wcgworld.com/"));
    }

    @Test
    public void testActivityLinkUnwinderProcessorUnwindable() throws Exception{
        testActivityUnwinderHelper(Lists.newArrayList("http://bit.ly/1cX5Rh4", "http://nope@#$%e"), Lists.newArrayList("http://www.wcgworld.com/"));
        testStringActivityUnwinderHelper(Lists.newArrayList("http://bit.ly/1cX5Rh4", "http://nope@#$%s"), Lists.newArrayList("http://www.wcgworld.com/"));
    }

    public void testActivityUnwinderHelper(List<String> input, List<String> expected) throws Exception{

        // Purge all of the domain wait times (for testing)
        LinkResolverHelperFunctions.purgeAllDomainWaitTimes();

        // Create a new activity
        Activity activity = new Activity();
        activity.setLinks(input);
        StreamsDatum datum = new StreamsDatum(activity);
        LinkResolverProcessor processor = new LinkResolverProcessor();
        processor.prepare(null);

        List<StreamsDatum> result = processor.process(datum);
        assertNotNull(result);
        assertEquals(1, result.size());
        StreamsDatum resultDatum = result.get(0);
        assertNotNull(resultDatum);
        assertTrue(resultDatum.getDocument() instanceof Activity);


        Activity resultActivity = (Activity) resultDatum.getDocument();
        assertNotNull(resultActivity.getLinks());
        List<String> resultLinks = resultActivity.getLinks();
        assertEquals(expected.size(), resultLinks.size());
        assertEquals(Sets.newHashSet(expected), Sets.newHashSet(resultLinks));
    }

    public void testStringActivityUnwinderHelper(List<String> input, List<String> expected) throws Exception{
        LinkResolverHelperFunctions.purgeAllDomainWaitTimes();
        Activity activity = new Activity();
        activity.setLinks(input);
        String str = StreamsJacksonMapper.getInstance().writeValueAsString(activity);
        StreamsDatum datum = new StreamsDatum(str);
        LinkResolverProcessor processor = new LinkResolverProcessor();
        processor.prepare(null);
        List<StreamsDatum> result = processor.process(datum);
        assertNotNull(result);
        assertEquals(1, result.size());
        StreamsDatum resultDatum = result.get(0);
        assertNotNull(resultDatum);
        assertTrue(resultDatum.getDocument() instanceof String);
        String resultActivityString = (String) resultDatum.getDocument();
        Activity resultActivity = StreamsJacksonMapper.getInstance().readValue(resultActivityString, Activity.class);
        assertNotNull(resultActivity.getLinks());
        List<String> resultLinks = resultActivity.getLinks();
        assertEquals(expected.size(), resultLinks.size());
        assertEquals(Sets.newHashSet(expected), Sets.newHashSet(resultLinks));
    }

}
