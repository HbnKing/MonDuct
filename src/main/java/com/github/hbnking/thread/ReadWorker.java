package com.github.hbnking.thread;

import com.github.hbnking.config.AppConfig;
import com.github.hbnking.datasource.MongoDBDataSource;
import com.github.hbnking.model.OplogEntry;
import com.github.hbnking.buffer.DisruptorBuffer;
import com.github.hbnking.buffer.PartitionStrategy;
import com.github.hbnking.filter.FilterUtils;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public class ReadWorker implements Callable<Void> {
    private static final Logger logger = LoggerFactory.getLogger(ReadWorker.class);
    private final MongoDBDataSource sourceDataSource;
    private final DisruptorBuffer disruptorBuffer;
    private final FilterUtils filterUtils;
    private final PartitionStrategy partitionStrategy;
    private final String collectionName;

    public ReadWorker(AppConfig config, DisruptorBuffer disruptorBuffer, FilterUtils filterUtils,
                      PartitionStrategy partitionStrategy, String collectionName) {
        this.sourceDataSource = new MongoDBDataSource(config.getSourceUri());
        this.disruptorBuffer = disruptorBuffer;
        this.filterUtils = filterUtils;
        this.partitionStrategy = partitionStrategy;
        this.collectionName = collectionName;
    }

    @Override
    public Void call() throws Exception {
        MongoCollection<Document> collection = sourceDataSource.getDatabase("").getCollection(collectionName);
        MongoCursor<Document> cursor = collection.find().iterator();
        try {
            while (cursor.hasNext()) {
                Document document = cursor.next();
                OplogEntry oplogEntry = new OplogEntry();
                oplogEntry.setFullDocument(document);
                oplogEntry.setNs(sourceDataSource.getDatabase("").getName() + "." + collectionName);
                if (filterUtils.shouldProcess(oplogEntry)) {
                    int partitionIndex = partitionStrategy.getPartitionIndex(oplogEntry, disruptorBuffer.getDisruptorCount());
                    disruptorBuffer.put(partitionIndex, oplogEntry);
                }
            }
        } finally {
            cursor.close();
        }
        return null;
    }
}