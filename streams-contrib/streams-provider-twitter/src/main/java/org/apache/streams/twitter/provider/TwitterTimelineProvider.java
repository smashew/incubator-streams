package org.apache.streams.twitter.provider;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.typesafe.config.Config;
import org.apache.streams.config.StreamsConfigurator;
import org.apache.streams.core.StreamsDatum;
import org.apache.streams.core.StreamsProvider;
import org.apache.streams.core.StreamsResultSet;
import org.apache.streams.twitter.TwitterStreamConfiguration;
import org.apache.streams.util.ComponentUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;

public class TwitterTimelineProvider implements StreamsProvider, Serializable {

    public final static String STREAMS_ID = "TwitterTimelineProvider";

    private final static Logger LOGGER = LoggerFactory.getLogger(TwitterTimelineProvider.class);

    private TwitterStreamConfiguration config;

    private Class klass;

    public TwitterStreamConfiguration getConfig() {
        return config;
    }

    public void setConfig(TwitterStreamConfiguration config) {
        this.config = config;
    }

    protected final Queue<StreamsDatum> providerQueue = new ArrayBlockingQueue<StreamsDatum>(5000);

    protected int idsCount;
    protected Iterator<Long> ids;

    protected ListeningExecutorService executor;

    protected DateTime start;
    protected DateTime end;

    Boolean jsonStoreEnabled;
    Boolean includeEntitiesEnabled;

    private static ExecutorService newFixedThreadPoolWithQueueSize(int nThreads) {
        return new ThreadPoolExecutor(
                nThreads,
                nThreads,
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(nThreads),
                new RejectedExecutionHandler() {
                    public synchronized void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        // Wait until the pool is free for another item

                        if (executor.getMaximumPoolSize() == executor.getQueue().size()) {
                            while (executor.getMaximumPoolSize() == executor.getQueue().size())
                                safeSleep();
                        }
                        executor.submit(r);
                    }

                    public void safeSleep() {
                        try {
                            // wait one tenth of a millisecond
                            Thread.yield();
                            Thread.sleep(5);
                        }
                        catch(Exception e) {
                            /* no op */
                        }
                    }
                });
    }

    public TwitterTimelineProvider() {
        Config config = StreamsConfigurator.config.getConfig("twitter");
        this.config = TwitterStreamConfigurator.detectConfiguration(config);
    }

    public TwitterTimelineProvider(TwitterStreamConfiguration config) {
        this.config = config;
    }

    public TwitterTimelineProvider(Class klass) {
        Config config = StreamsConfigurator.config.getConfig("twitter");
        this.config = TwitterStreamConfigurator.detectConfiguration(config);
        this.klass = klass;
    }

    public TwitterTimelineProvider(TwitterStreamConfiguration config, Class klass) {
        this.config = config;
        this.klass = klass;
    }

    public Queue<StreamsDatum> getProviderQueue() {
        return this.providerQueue;
    }

    @Override
    public void startStream() {
        LOGGER.debug("{} startStream", STREAMS_ID);
        throw new org.apache.commons.lang.NotImplementedException();
    }

    protected void captureTimeline(long currentId) {

        Paging paging = new Paging(1, 200);
        List<Status> statuses = null;

        do
        {
            Twitter client = getTwitterClient();

            int keepTrying = 0;

            // keep trying to load, give it 5 attempts.
            while (keepTrying < 5) {
                try {

                    LOGGER.info("Using Twitter Client: {}", client.getOAuthAccessToken());
                    statuses = client.getUserTimeline(currentId, paging);

                    TwitterErrorHandler.resetBackOff();

                    for (Status tStat : statuses) {
                        if(shouldEmit(tStat)) {
                            String json = TwitterObjectFactory.getRawJSON(tStat);
                            ComponentUtils.offerUntilSuccess(new StreamsDatum(json), providerQueue);
                        }
                    }

                    paging.setPage(paging.getPage() + 1);
                    keepTrying = 10;
                }
                catch(TwitterException twitterException) {
                    keepTrying += TwitterErrorHandler.handleTwitterError(client, twitterException);
                }
                catch(Exception e) {
                    keepTrying += TwitterErrorHandler.handleTwitterError(client, e);
                }
            }
        }
        while (shouldContinuePulling(statuses));
    }

    protected boolean shouldEmit(Status status) {
        return true;
    }

    protected boolean shouldContinuePulling(List<Status> statuses) {
        return (statuses != null) && (statuses.size() > 0);
    }

    public StreamsResultSet readCurrent() {
        LOGGER.debug("{} readCurrent", STREAMS_ID);

        Preconditions.checkArgument(ids.hasNext());

        synchronized( TwitterTimelineProvider.class ) {

            while( ids.hasNext() ) {
                Long currentId = ids.next();
                LOGGER.info("Provider Task Starting: {}", currentId);
                captureTimeline(currentId);
            }

        }

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
        throw new NotImplementedException();
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

        executor = MoreExecutors.listeningDecorator(newFixedThreadPoolWithQueueSize(3));

        Preconditions.checkNotNull(providerQueue);
        Preconditions.checkNotNull(this.klass);
        Preconditions.checkNotNull(config.getOauth().getConsumerKey());
        Preconditions.checkNotNull(config.getOauth().getConsumerSecret());
        Preconditions.checkNotNull(config.getOauth().getAccessToken());
        Preconditions.checkNotNull(config.getOauth().getAccessTokenSecret());
        Preconditions.checkNotNull(config.getFollow());

        idsCount = config.getFollow().size();
        ids = config.getFollow().iterator();

        jsonStoreEnabled = Optional.fromNullable(Boolean.parseBoolean(config.getJsonStoreEnabled())).or(true);
        includeEntitiesEnabled = Optional.fromNullable(Boolean.parseBoolean(config.getIncludeEntities())).or(true);
    }


    protected Twitter getTwitterClient()
    {
        String baseUrl = "https://api.twitter.com:443/1.1/";

        ConfigurationBuilder builder = new ConfigurationBuilder()
                .setOAuthConsumerKey(config.getOauth().getConsumerKey())
                .setOAuthConsumerSecret(config.getOauth().getConsumerSecret())
                .setOAuthAccessToken(config.getOauth().getAccessToken())
                .setOAuthAccessTokenSecret(config.getOauth().getAccessTokenSecret())
                .setIncludeEntitiesEnabled(includeEntitiesEnabled)
                .setJSONStoreEnabled(jsonStoreEnabled)
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
