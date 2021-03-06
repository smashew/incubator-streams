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
package org.apache.streams.twitter.provider;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.typesafe.config.Config;
import org.apache.streams.config.StreamsConfigurator;
import org.apache.streams.core.StreamsDatum;
import org.apache.streams.core.StreamsProvider;
import org.apache.streams.core.StreamsResultSet;
import org.apache.streams.twitter.TwitterUserInformationConfiguration;
import org.apache.streams.util.ComponentUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;

public class TwitterUserInformationProvider implements StreamsProvider, Serializable
{

    public static final String STREAMS_ID = "TwitterUserInformationProvider";
    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterUserInformationProvider.class);

    private TwitterUserInformationConfiguration twitterUserInformationConfiguration;

    private final Queue<StreamsDatum> providerQueue = new ArrayBlockingQueue<StreamsDatum>(1000);

    public TwitterUserInformationConfiguration getConfig()              { return twitterUserInformationConfiguration; }

    public void setConfig(TwitterUserInformationConfiguration config)   { this.twitterUserInformationConfiguration = config; }

    protected Iterator<Long[]> idsBatches;
    protected Iterator<String[]> screenNameBatches;

    protected ListeningExecutorService executor;

    protected DateTime start;
    protected DateTime end;

    private static ExecutorService newFixedThreadPoolWithQueueSize(int nThreads, int queueSize) {
        return new ThreadPoolExecutor(nThreads, nThreads,
                5000L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(queueSize, true), new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public TwitterUserInformationProvider() {
        Config config = StreamsConfigurator.config.getConfig("twitter");
        this.twitterUserInformationConfiguration = TwitterStreamConfigurator.detectTwitterUserInformationConfiguration(config);

    }

    public TwitterUserInformationProvider(TwitterUserInformationConfiguration config) {
        this.twitterUserInformationConfiguration = config;
    }

    public Queue<StreamsDatum> getProviderQueue() {
        return this.providerQueue;
    }

    @Override
    public void startStream() {
        // no op
    }


    protected void loadBatch(Long[] ids) {
        int keepTrying = 0;

        // keep trying to load, give it 5 attempts.
        while (keepTrying < 5) {
            Twitter client = getTwitterClient();
            try
            {
                long[] toQuery = new long[ids.length];
                for(int i = 0; i < ids.length; i++)
                    toQuery[i] = ids[i];

                for (User tStat : client.lookupUsers(toQuery)) {
                    String json = TwitterObjectFactory.getRawJSON(tStat);
                    ComponentUtils.offerUntilSuccess(new StreamsDatum(json), providerQueue);
                }
                keepTrying = 10;
            }
            catch(TwitterException twitterException) {
                keepTrying += TwitterErrorHandler.handleTwitterError(client, twitterException);
            }
            catch(Exception e) {
                keepTrying += TwitterErrorHandler.handleTwitterError(client, e);
                e.printStackTrace();
            }
        }
    }

    protected void loadBatch(String[] ids) {
        int keepTrying = 0;
        LOGGER.info("Starting batch of ScreenNames: {}", ids);

        // keep trying to load, give it 5 attempts.
        while (keepTrying < 5) {
            Twitter client = getTwitterClient();
            try
            {
                for (User tStat : client.lookupUsers(ids)) {
                    LOGGER.info("Offering: {}", tStat.getScreenName());
                    ComponentUtils.offerUntilSuccess(new StreamsDatum(TwitterObjectFactory.getRawJSON(tStat)), providerQueue);
                }
                keepTrying = 10;
            }
            catch(TwitterException twitterException) {
                keepTrying += TwitterErrorHandler.handleTwitterError(client, twitterException);
            }
            catch(Exception e) {
                keepTrying += TwitterErrorHandler.handleTwitterError(client, e);
                e.printStackTrace();
            }
        }
        LOGGER.info("Completed batch of ScreenNames: {}", ids);

    }

    public StreamsResultSet readCurrent() {

        Preconditions.checkArgument(idsBatches.hasNext() || screenNameBatches.hasNext());

        LOGGER.info("readCurrent");

        while(idsBatches.hasNext())
            loadBatch(idsBatches.next());

        while(screenNameBatches.hasNext())
            loadBatch(screenNameBatches.next());


        LOGGER.info("Finished.  Cleaning up...");

        LOGGER.info("Providing {} docs", providerQueue.size());

        StreamsResultSet result =  new StreamsResultSet(providerQueue);

        LOGGER.info("Exiting");

        return result;

    }

    public StreamsResultSet readNew(BigInteger sequence) {
        LOGGER.debug("{} readNew", STREAMS_ID);
        throw new NotImplementedException();
    }

    public StreamsResultSet readRange(DateTime start, DateTime end) {
        LOGGER.debug("{} readRange", STREAMS_ID);
        this.start = start;
        this.end = end;
        readCurrent();
        StreamsResultSet result = (StreamsResultSet)providerQueue.iterator();
        return result;
    }

    void shutdownAndAwaitTermination(ExecutorService pool) {
        pool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
                pool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(10, TimeUnit.SECONDS))
                    System.err.println("Pool did not terminate");
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void prepare(Object o) {

        executor = MoreExecutors.listeningDecorator(newFixedThreadPoolWithQueueSize(5, 20));

        Preconditions.checkNotNull(providerQueue);
        Preconditions.checkNotNull(twitterUserInformationConfiguration.getOauth().getConsumerKey());
        Preconditions.checkNotNull(twitterUserInformationConfiguration.getOauth().getConsumerSecret());
        Preconditions.checkNotNull(twitterUserInformationConfiguration.getOauth().getAccessToken());
        Preconditions.checkNotNull(twitterUserInformationConfiguration.getOauth().getAccessTokenSecret());
        Preconditions.checkNotNull(twitterUserInformationConfiguration.getInfo());

        List<String> screenNames = new ArrayList<String>();
        List<String[]> screenNameBatches = new ArrayList<String[]>();

        List<Long> ids = new ArrayList<Long>();
        List<Long[]> idsBatches = new ArrayList<Long[]>();

        for(String s : twitterUserInformationConfiguration.getInfo()) {
            if(s != null)
            {
                String potentialScreenName = s.replaceAll("@", "").trim().toLowerCase();

                // See if it is a long, if it is, add it to the user iD list, if it is not, add it to the
                // screen name list
                try {
                    ids.add(Long.parseLong(potentialScreenName));
                } catch (Exception e) {
                    screenNames.add(potentialScreenName);
                }

                // Twitter allows for batches up to 100 per request, but you cannot mix types

                if(ids.size() >= 100) {
                    // add the batch
                    idsBatches.add(ids.toArray(new Long[ids.size()]));
                    // reset the Ids
                    ids = new ArrayList<Long>();
                }

                if(screenNames.size() >= 100) {
                    // add the batch
                    screenNameBatches.add(screenNames.toArray(new String[ids.size()]));
                    // reset the Ids
                    screenNames = new ArrayList<String>();
                }
            }
        }


        if(ids.size() > 0)
            idsBatches.add(ids.toArray(new Long[ids.size()]));

        if(screenNames.size() > 0)
            screenNameBatches.add(screenNames.toArray(new String[ids.size()]));

        this.idsBatches = idsBatches.iterator();
        this.screenNameBatches = screenNameBatches.iterator();
    }

    protected Twitter getTwitterClient()
    {
        String baseUrl = "https://api.twitter.com:443/1.1/";

        ConfigurationBuilder builder = new ConfigurationBuilder()
                .setOAuthConsumerKey(twitterUserInformationConfiguration.getOauth().getConsumerKey())
                .setOAuthConsumerSecret(twitterUserInformationConfiguration.getOauth().getConsumerSecret())
                .setOAuthAccessToken(twitterUserInformationConfiguration.getOauth().getAccessToken())
                .setOAuthAccessTokenSecret(twitterUserInformationConfiguration.getOauth().getAccessTokenSecret())
                .setIncludeEntitiesEnabled(true)
                .setJSONStoreEnabled(true)
                .setAsyncNumThreads(3)
                .setRestBaseURL(baseUrl)
                .setIncludeMyRetweetEnabled(Boolean.TRUE)
                .setPrettyDebugEnabled(Boolean.TRUE);

        return new TwitterFactory(builder.build()).getInstance();
    }

    @Override
    public void cleanUp() {
        shutdownAndAwaitTermination(executor);
    }
}
