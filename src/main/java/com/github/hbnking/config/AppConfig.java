package com.github.hbnking.config;

import com.github.hbnking.sync.SyncLevel;
import com.github.hbnking.sync.SyncMode;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "mongodb.sync")
@Data
public class AppConfig {
    private static final Logger logger = LoggerFactory.getLogger(AppConfig.class);


    // 同步级别
    private SyncLevel  syncLevel ;
    // 同步模式，默认为 full
    private SyncMode syncMode ;
    // 源 MongoDB 连接 URI
    private String sourceUri;
    // 目标 MongoDB 连接 URI
    private String targetUri;
    // 缓冲区大小，默认为 10000
    private int bufferSize = 16384;
    // 读取线程数量，默认为 4
    private int readThreads = 4;
    // 写入线程数量，默认为 4
    private int writeThreads = 4;
    // 是否同步索引，默认为 true
    private boolean shouldSyncIndexes = true;
    // 是否启用监控，默认为 true
    private boolean monitorEnabled = true;
    // 监控间隔时间（毫秒），默认为 5000
    private long monitorInterval = 5000;
    // 延迟同步时间（毫秒），默认为 0
    private long delaySync = 0;
    // 是否同步 DDL 操作，默认为 false
    private boolean syncDdl = false;
    // 是否在同步 60% 数据后再同步索引，默认为 true
    private boolean syncIndexAfter60Percent = true;
    // 是否启用多表并行同步，默认为 true
    private boolean multiTableParallel = true;
    // Disruptor 实例数量，默认为 8
    private int disruptorCount = 8;
    // 是否执行全量同步，默认为 true
    private boolean fullSync = true;
    // 包含的数据库列表，多个数据库用逗号分隔，默认为空字符串
    private String includeDatabases = "";
    // 排除的数据库列表，多个数据库用逗号分隔，默认为空字符串
    private String excludeDatabases = "";
    // 包含的表列表，多个表用逗号分隔，默认为空字符串
    private String includeTables = "";
    // 排除的表列表，多个表用逗号分隔，默认为空字符串
    private String excludeTables = "";
    // 包含的命名空间列表，多个命名空间用逗号分隔，默认为空字符串
    private String[] namespaces = new String[]{};
    // 命名空间的正则表达式过滤规则，默认为空字符串
    private String regexNamespaces = "";
    // 最大重试次数，默认为 3
    private int maxRetries = 3;
    // 重试延迟时间（毫秒），默认为 2000
    private long retryDelay = 2000;
    // 是否启用数据验证，默认为 false
    private boolean dataVerificationEnabled = false;
    // 数据验证间隔时间（毫秒），默认为 60000
    private long dataVerificationInterval = 60000;

    public void logConfig() {
        logger.info("Sync Mode: {}", syncMode);
        logger.info("Source URI: {}", sourceUri);
        logger.info("Target URI: {}", targetUri);
        logger.info("Buffer Size: {}", bufferSize);
        logger.info("Read Threads: {}", readThreads);
        logger.info("Write Threads: {}", writeThreads);
        logger.info("Should Sync Indexes: {}", shouldSyncIndexes);
        logger.info("Monitor Enabled: {}", monitorEnabled);
        logger.info("Monitor Interval: {}", monitorInterval);
        logger.info("Delay Sync: {}", delaySync);
        logger.info("Sync DDL: {}", syncDdl);
        logger.info("Sync Index After 60 Percent: {}", syncIndexAfter60Percent);
        logger.info("Multi Table Parallel: {}", multiTableParallel);
        logger.info("Disruptor Count: {}", disruptorCount);
        logger.info("Full Sync: {}", fullSync);
        logger.info("Include Databases: {}", includeDatabases);
        logger.info("Exclude Databases: {}", excludeDatabases);
        logger.info("Include Tables: {}", includeTables);
        logger.info("Exclude Tables: {}", excludeTables);
        logger.info("Namespaces: {}", namespaces);
        logger.info("Regex Namespaces: {}", regexNamespaces);
        logger.info("Max Retries: {}", maxRetries);
        logger.info("Retry Delay: {}", retryDelay);
        logger.info("Data Verification Enabled: {}", dataVerificationEnabled);
        logger.info("Data Verification Interval: {}", dataVerificationInterval);
    }
}