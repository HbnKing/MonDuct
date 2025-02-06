package com.github.hbnking.thread;

import com.github.hbnking.buffer.DisruptorBuffer;
import com.github.hbnking.buffer.PartitionStrategy;
import com.github.hbnking.config.AppConfig;
import com.github.hbnking.filter.FilterUtils;
import com.github.hbnking.sync.ChangeStreamSync;
import com.github.hbnking.sync.DisruptorWriter;
import com.github.hbnking.sync.FullSync;
import com.github.hbnking.sync.OplogSync;
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
    private final DisruptorWriter disruptorWriter;

    public ThreadManager(AppConfig config, FullSync fullSync, OplogSync oplogSync,
                         ChangeStreamSync changeStreamSync, DisruptorWriter disruptorWriter) {
        // 根据配置文件中的线程数量创建固定大小的线程池
        this.executorService = Executors.newFixedThreadPool(config.getReadThreads() + config.getWriteThreads());
        this.fullSync = fullSync;
        this.oplogSync = oplogSync;
        this.changeStreamSync = changeStreamSync;
        this.disruptorWriter = disruptorWriter;
    }

    public ThreadManager(AppConfig config, DisruptorBuffer disruptorBuffer, FilterUtils filterUtils, PartitionStrategy partitionStrategy) {
        this.executorService = Executors.newFixedThreadPool(config.getReadThreads() + config.getWriteThreads());
        this.fullSync = null;
        this.oplogSync = null;
        this.changeStreamSync = null;
        this.disruptorWriter = null;
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
     * 启动 Disruptor 写入任务
     */
    public void startDisruptorWriter() {
        executorService.submit(() -> {
            try {
                logger.info("Starting disruptor writer task...");
                disruptorWriter.start();
                logger.info("Disruptor writer task has been started.");
            } catch (Exception e) {
                logger.error("Error starting disruptor writer: ", e);
            }
        });
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