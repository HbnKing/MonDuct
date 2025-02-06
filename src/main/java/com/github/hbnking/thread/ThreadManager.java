package com.github.hbnking.thread;

import com.github.hbnking.buffer.DisruptorBuffer;
import com.github.hbnking.buffer.PartitionStrategy;
import com.github.hbnking.config.AppConfig;
import com.github.hbnking.datasource.MongoDBDataSource;
import com.github.hbnking.filter.FilterUtils;
import com.github.hbnking.sync.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadManager {
    private static final Logger logger = LoggerFactory.getLogger(ThreadManager.class);
    private final ExecutorService executorService;
    private final FullSync fullSync;
    private final OplogSync oplogSync;
    private final ChangeStreamSync changeStreamSync;


    private final MongoDBDataSource sourceDataSource;
    private final DisruptorBuffer disruptorBuffer;
    private final FilterUtils filterUtils;
    private final PartitionStrategy partitionStrategy;


    private final AppConfig  config ;
    public ThreadManager(AppConfig config, MongoDBDataSource sourceDataSource,
                         DisruptorBuffer disruptorBuffer, FilterUtils filterUtils,
                         PartitionStrategy partitionStrategy) {

        this.sourceDataSource = sourceDataSource;
        this.disruptorBuffer = disruptorBuffer;
        this.filterUtils = filterUtils;
        this.partitionStrategy = partitionStrategy;

        this.config = config ;
        // 根据配置文件中的线程数量创建固定大小的线程池
        this.executorService = Executors.newFixedThreadPool(config.getReadThreads() + config.getWriteThreads());

        // 在内部创建 FullSync 对象
        this.fullSync = new FullSync(config, sourceDataSource, disruptorBuffer, filterUtils, partitionStrategy);

        // 在内部创建 OplogSync 对象
        this.oplogSync = new OplogSync(config, sourceDataSource, disruptorBuffer, filterUtils, partitionStrategy);

        // 在内部创建 ChangeStreamSync 对象
        this.changeStreamSync = new ChangeStreamSync(config, sourceDataSource, disruptorBuffer, filterUtils, partitionStrategy);


    }

    /**
     * 启动全量同步任务
     */
    public void startFullSync() {
        executorService.submit(() -> {
            try {
                logger.info("Starting full sync task...");
                fullSync.start();
                logger.info("Full sync task has been started.");
            } catch (Exception e) {
                logger.error("Error starting full sync: ", e);
            }
        });
    }

    /**
     * 启动 Oplog 同步任务
     */
    public void startOplogSync() {
        executorService.submit(() -> {
            try {
                logger.info("Starting oplog sync task...");
                oplogSync.start();
                logger.info("Oplog sync task has been started.");
            } catch (Exception e) {
                logger.error("Error starting oplog sync: ", e);
            }
        });
    }

    /**
     * 启动 Change Stream 同步任务
     */
    public void startChangeStreamSync() {
        executorService.submit(() -> {
            try {
                logger.info("Starting change stream sync task...");
                changeStreamSync.startSync();
                logger.info("Change stream sync task has been started.");
            } catch (Exception e) {
                logger.error("Error starting change stream sync: ", e);
            }
        });
    }



    /**
     * 停止所有同步任务
     */
    public void stopAllSyncTasks() {
        try {
           /* // 停止 DisruptorWriter
            if (disruptorWriter != null) {
                disruptorWriter.stop();
            }

            // 停止 ChangeStreamSync
            if (changeStreamSync != null) {
                changeStreamSync.stopSync();
            }

            // 停止 OplogSync
            if (oplogSync != null) {
                oplogSync.stop();
            }

            // 停止 FullSync（这里假设 FullSync 有 stop 方法）
            if (fullSync != null) {
                fullSync.stop();
            }
*/
            // 关闭线程池
            executorService.shutdownNow();
            logger.info("All sync tasks have been stopped.");
        } catch (Exception e) {
            logger.error("Error stopping sync tasks: ", e);
        }
    }



    public void startSyncTask() {
        SyncMode syncMode = config.getSyncMode();
       if (true) {
            OplogSync oplogSync = new OplogSync(config, sourceDataSource, this.disruptorBuffer, filterUtils, partitionStrategy);
            startOplogSync();
        } else if ("changeStream".equals(syncMode)) {
            ChangeStreamSync changeStreamSync = new ChangeStreamSync(config, disruptorBuffer, filterUtils, partitionStrategy);
            startChangeStreamSync();
        } else if ("full".equals(syncMode)) {
            FullSync fullSync = new FullSync(config, sourceDataSource, disruptorBuffer, filterUtils, partitionStrategy);
            startFullSync();
        } else {
            logger.error("Invalid sync mode: {}", syncMode);
        }
    }

    /**
     * 关闭线程池，停止所有同步任务
     */
    public void shutdown() {
        try {
            logger.info("Shutting down thread manager...");
            executorService.shutdown();
            logger.info("Thread manager has been shut down.");
        } catch (Exception e) {
            logger.error("Error shutting down thread manager: ", e);
        }
    }
}