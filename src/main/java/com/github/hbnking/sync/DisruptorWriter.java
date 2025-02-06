package com.github.hbnking.sync;

import com.github.hbnking.config.AppConfig;
import com.github.hbnking.datasource.MongoDBDataSource;
import com.github.hbnking.model.OplogEntry;
import com.github.hbnking.model.OperationType;
import com.github.hbnking.buffer.DisruptorBuffer;
import com.github.hbnking.buffer.DisruptorBuffer.OplogEntryEvent;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DisruptorWriter {
    private static final Logger logger = LoggerFactory.getLogger(DisruptorWriter.class);
    private final MongoDBDataSource targetDataSource;
    private final List<Disruptor<OplogEntryEvent>> disruptors;
    private final ExecutorService executorService;

    public DisruptorWriter(AppConfig config, DisruptorBuffer buffer) {
        this.targetDataSource = new MongoDBDataSource(config.getTargetUri());
        this.disruptors = buffer.getDisruptors();
        this.executorService = Executors.newFixedThreadPool(config.getWriteThreads());
        setupEventHandlers();
    }

    private void setupEventHandlers() {
        for (Disruptor<OplogEntryEvent> disruptor : disruptors) {
            try {
                disruptor.handleEventsWith(new OplogEntryEventHandler());
                executorService.submit(disruptor::start);
                logger.info("Event handler set up and Disruptor started for a disruptor instance.");
            } catch (Exception e) {
                logger.error("Failed to set up event handler and start Disruptor: {}", e.getMessage(), e);
            }
        }
    }

    public void start() {
        logger.info("DisruptorWriter started. Ready to process OplogEntry events.");
    }

    public void shutdown() {
        try {
            for (Disruptor<OplogEntryEvent> disruptor : disruptors) {
                disruptor.shutdown();
            }
            executorService.shutdown();
            targetDataSource.close();
            logger.info("DisruptorWriter shut down successfully.");
        } catch (Exception e) {
            logger.error("Error while shutting down DisruptorWriter: {}", e.getMessage(), e);
        }
    }

    public class OplogEntryEventHandler implements EventHandler<OplogEntryEvent>{
        public void onEvent(OplogEntryEvent event, long sequence, boolean endOfBatch) {
            try {
                OplogEntry oplogEntry = event.getOplogEntry();
                if (oplogEntry != null) {
                    processOplogEntry(oplogEntry);
                }
            } catch (Exception e) {
                logger.error("Error processing OplogEntry event: {}", e.getMessage(), e);
            }
        }

        private void processOplogEntry(OplogEntry oplogEntry) {
            try {
                String namespace = oplogEntry.getNs();
                String[] parts = namespace.split("\\.");
                if (parts.length != 2) {
                    logger.error("Invalid namespace: {}", namespace);
                    return;
                }
                String databaseName = parts[0];
                String collectionName = parts[1];

                MongoDatabase database = targetDataSource.getDatabase(parts[0]);

                if (database == null) {
                    logger.error("Database {} not found in target MongoDB.", databaseName);
                    return;
                }

                MongoCollection<Document> collection = database.getCollection(collectionName);

                OperationType operationType = oplogEntry.getOp();
                switch (operationType) {
                    case INSERT:
                        collection.insertOne(oplogEntry.getFullDocument());
                        logger.info("Inserted document with ID: {}", oplogEntry.getFullDocument().get("_id"));
                        break;
                    case UPDATE:
                        Document filter = oplogEntry.getDocumentKey();
                        collection.replaceOne(filter, oplogEntry.getFullDocument());
                        logger.info("Updated document with ID: {}", oplogEntry.getFullDocument().get("_id"));
                        break;
                    case DELETE:
                        collection.deleteOne(oplogEntry.getDocumentKey());
                        logger.info("Deleted document with ID: {}", oplogEntry.getFullDocument().get("_id"));
                        break;
                    default:
                        logger.warn("Unsupported operation type: {}", operationType);
                }
            } catch (Exception e) {
                logger.error("Error processing OplogEntry (op: {}, ns: {}): {}", oplogEntry.getOp(), oplogEntry.getNs(), e.getMessage(), e);
            }
        }
    }
}