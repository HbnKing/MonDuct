package com.github.hbnking.sync;

import com.github.hbnking.config.AppConfig;
import com.github.hbnking.datasource.MongoDBDataSource;
import com.github.hbnking.model.OplogEntry;
import com.github.hbnking.buffer.DisruptorBuffer;
import com.github.hbnking.buffer.PartitionStrategy;
import com.github.hbnking.filter.FilterUtils;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class OplogSync  implements IRead {
    private static final Logger logger = LoggerFactory.getLogger(OplogSync.class);
    private final AppConfig config;
    private final MongoDBDataSource sourceDataSource;
    private final DisruptorBuffer disruptorBuffer;
    private final FilterUtils filterUtils;
    private final PartitionStrategy partitionStrategy;
    private final ExecutorService executorService;

    public OplogSync(AppConfig config, MongoDBDataSource sourceDataSource, DisruptorBuffer disruptorBuffer,
                     FilterUtils filterUtils, PartitionStrategy partitionStrategy) {
        this.config = config;
        this.sourceDataSource = sourceDataSource;
        this.disruptorBuffer = disruptorBuffer;
        this.filterUtils = filterUtils;
        this.partitionStrategy = partitionStrategy;
        this.executorService = Executors.newSingleThreadExecutor();
    }

    public void start() {
        executorService.submit(() -> {
            try (MongoClient client = sourceDataSource.getClient()) {
                MongoDatabase localDb = client.getDatabase("local");
                MongoCollection<Document> oplogCollection = localDb.getCollection("oplog.rs");

                // 可以根据需要添加过滤条件，这里简单示例从最新的 oplog 开始同步
                Document lastOplog = oplogCollection.find().sort(new Document("$natural", -1)).limit(1).first();
                if (lastOplog != null) {
                    Bson query = Filters.gt("ts", lastOplog.get("ts"));

                    // 开始监听 oplog 变化
                    for (Document oplogDoc : oplogCollection.find(query).cursorType(com.mongodb.CursorType.TailableAwait)) {
                        OplogEntry oplogEntry = OplogEntry.fromOplog(oplogDoc);
                        if (oplogEntry != null && filterUtils.shouldProcess(oplogEntry)) {
                            int partition = partitionStrategy.getPartitionIndex(oplogEntry,10);
                            disruptorBuffer.getRingBuffer(partition).publishEvent((event, sequence) ->
                                    event.setOplogEntry(oplogEntry));
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Error in Oplog sync: {}", e.getMessage(), e);
            }
        });
    }

    @Override
    public void context(DisruptorBuffer buffer) {

    }

    @Override
    public void read() {

    }
}