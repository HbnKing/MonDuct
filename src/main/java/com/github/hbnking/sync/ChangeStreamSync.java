package com.github.hbnking.sync;

import com.github.hbnking.config.AppConfig;
import com.github.hbnking.model.OplogEntry;
import com.github.hbnking.buffer.DisruptorBuffer;
import com.github.hbnking.buffer.PartitionStrategy;
import com.github.hbnking.filter.FilterUtils;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.in;

public class ChangeStreamSync {
    private static final Logger logger = LoggerFactory.getLogger(ChangeStreamSync.class);
    private final AppConfig config;
    private final DisruptorBuffer disruptorBuffer;
    private final FilterUtils filterUtils;
    private final PartitionStrategy partitionStrategy;
    private final ExecutorService executorService;

    public ChangeStreamSync(AppConfig config, DisruptorBuffer disruptorBuffer, FilterUtils filterUtils,
                            PartitionStrategy partitionStrategy) {
        this.config = config;
        this.disruptorBuffer = disruptorBuffer;
        this.filterUtils = filterUtils;
        this.partitionStrategy = partitionStrategy;
        this.executorService = Executors.newSingleThreadExecutor();
    }

    public void startSync() {
        executorService.submit(() -> {
            try (MongoClient client = MongoClients.create(config.getSourceUri())) {
                // todo 监听级别



                Bson pipeline = match(in("operationType", "insert", "update", "delete"));
                ChangeStreamIterable<Document> changeStream = client.watch(Arrays.asList(new Bson[]{pipeline}));

                for (ChangeStreamDocument changeStreamDocument : changeStream) {
                    OplogEntry oplogEntry = OplogEntry.fromChangeStream(changeStreamDocument);
                    if (oplogEntry != null && filterUtils.shouldProcess(oplogEntry)) {
                        int partition = partitionStrategy.getPartitionIndex(oplogEntry,8);
                        disruptorBuffer.getRingBuffer(partition).publishEvent((event, sequence) ->
                                event.setOplogEntry(oplogEntry));
                    }
                }
            } catch (Exception e) {
                logger.error("Error in Change Stream sync: {}", e.getMessage(), e);
            }
        });
    }
}