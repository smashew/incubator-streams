package org.apache.streams.elasticsearch;

import org.apache.streams.core.*;
import org.apache.streams.util.ComponentUtils;
import org.elasticsearch.search.SearchHit;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.*;

/**
 * ***********************************************************************************************************
 * Authors:
 * smashew
 * steveblackmon
 * ************************************************************************************************************
 */

public class ElasticsearchPersistReader implements StreamsPersistReader, Serializable {
    public static final String STREAMS_ID = "ElasticsearchPersistReader";

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchPersistReader.class);

    protected final Queue<StreamsDatum> persistQueue = new ArrayBlockingQueue<StreamsDatum>(100);
    private final StreamsResultSet streamsResultSet = new StreamsResultSet(this.persistQueue, true);

    private ElasticsearchQuery elasticsearchQuery;
    private final ElasticsearchReaderConfiguration config;
    private final ElasticsearchClientManager elasticsearchClientManager;

    public ElasticsearchPersistReader(ElasticsearchReaderConfiguration config) {
        this(config, new ElasticsearchClientManager(config));
    }

    public ElasticsearchPersistReader(ElasticsearchReaderConfiguration config, ElasticsearchClientManager escm) {
        this.config = config;
        this.elasticsearchClientManager = escm;
    }

    @Override
    public void startStream() {
        LOGGER.debug("startStream");
        final ElasticsearchQuery query = this.elasticsearchQuery;

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (!query.isCompleted()) {
                        if(query.hasNext()) {
                            SearchHit hit = query.next();
                            StreamsDatum item = new StreamsDatum(hit.getSourceAsString(), hit.getId());
                            item.getMetadata().put("id", hit.getId());
                            item.getMetadata().put("index", hit.getIndex());
                            item.getMetadata().put("type", hit.getType());
                            ComponentUtils.offerUntilSuccess(item, streamsResultSet.getQueue());
                        }
                        else {
                            try {
                                Thread.sleep(1);
                            }
                            catch(InterruptedException ioe) {
                                LOGGER.error("sleep error: {}", ioe);
                            }
                        }
                    }
                }
                catch(Throwable e) {
                    LOGGER.error("Unexpected issue: {}", e.getMessage());
                }
                finally{
                    streamsResultSet.shutDown();
                }
            }
        }).start();
    }

    @Override
    public void prepare(Object o) {
        if(this.config == null)
            throw new RuntimeException("Unable to run without configuration");
        elasticsearchQuery = new ElasticsearchQuery(config, this.elasticsearchClientManager);
        elasticsearchQuery.execute(o);
        startStream();
    }

    @Override
    public StreamsResultSet readAll() {
        return readCurrent();
    }

    @Override
    public StreamsResultSet readCurrent() {
        return this.streamsResultSet;
    }

    //TODO - This just reads current records and does not adjust any queries
    @Override
    public StreamsResultSet readNew(BigInteger sequence) {
        return readCurrent();
    }

    //TODO - This just reads current records and does not adjust any queries
    @Override
    public StreamsResultSet readRange(DateTime start, DateTime end) {
        return readCurrent();
    }

    @Override
    public void cleanUp() {
        LOGGER.info("PersistReader done");
    }
}


