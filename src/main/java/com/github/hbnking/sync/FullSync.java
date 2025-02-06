package com.github.hbnking.sync;

import com.github.hbnking.config.AppConfig;
import com.github.hbnking.datasource.MongoDBDataSource;
import com.github.hbnking.datasource.NameSpaceRange;
import com.github.hbnking.datasource.Range;
import com.github.hbnking.datasource.SpliceNsData;
import com.github.hbnking.model.OplogEntry;
import com.github.hbnking.model.OperationType;
import com.github.hbnking.buffer.DisruptorBuffer;
import com.github.hbnking.buffer.PartitionStrategy;
import com.github.hbnking.filter.FilterUtils;
import com.mongodb.MongoNamespace;
import com.mongodb.client.*;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class FullSync implements IRead{
    private static final Logger logger = LoggerFactory.getLogger(FullSync.class);
    private final AppConfig config;
    private final MongoDBDataSource sourceDataSource;
    private final DisruptorBuffer disruptorBuffer;
    private final FilterUtils filterUtils;
    private final PartitionStrategy partitionStrategy;

    public FullSync(AppConfig config, MongoDBDataSource sourceDataSource,
                    DisruptorBuffer disruptorBuffer, FilterUtils filterUtils, PartitionStrategy partitionStrategy) {
        this.config = config;
        this.sourceDataSource = sourceDataSource;
        this.disruptorBuffer = disruptorBuffer;
        this.filterUtils = filterUtils;
        this.partitionStrategy = partitionStrategy;
    }

    public void start() {
        logger.info("Full sync process started.");

        read();
    }

    @Override
    public void context(DisruptorBuffer buffer) {

    }

    @Override
    public void read() {

        try (MongoClient sourceClient = sourceDataSource.getClient()) {

            SpliceNsData spliceNsData = SpliceNsData.init(sourceClient);
            while (spliceNsData.hasNext()){
                NameSpaceRange next = spliceNsData.getNext();
                Range range = next.getRange();
                MongoNamespace namespace = next.getNamespace();

                Document conditon = spliceNsData.getConditon(range);

                FindIterable<Document> documents = sourceClient.getDatabase(namespace.getDatabaseName()).getCollection(namespace.getCollectionName()).find(conditon).allowDiskUse(true);
                MongoCursor<Document> iterator = documents.iterator();
                while (iterator.hasNext()){

                    Document document = iterator.next();

                    OplogEntry oplogEntry = OplogEntry.fromOriginal(document ,namespace);

                    // 检查是否需要处理该文档
                    if (filterUtils.shouldProcess(oplogEntry)) {
                        int partition = partitionStrategy.getPartitionIndex(oplogEntry,config.getDisruptorCount());
                        disruptorBuffer.getRingBuffer(partition).publishEvent((event, sequence) ->
                                event.setOplogEntry(oplogEntry));
                    }
                }
            }
            logger.info("Full sync process completed.");
        } catch (Exception e) {
            logger.error("Error occurred during full sync: {}", e.getMessage(), e);
        }

    }
}