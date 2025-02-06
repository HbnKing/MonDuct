package com.github.hbnking.sync;

/**
 * 同步级别枚举，用于区分不同粒度的同步操作
 */
public enum SyncLevel {
    /**
     * 集群级别同步，同步整个 MongoDB 集群的数据
     */
    CLUSTER_LEVEL_SYNC("ClusterLevelSync", "集群级别同步，将对整个集群的数据进行同步操作。"),
    /**
     * 库级别同步，仅同步指定数据库的数据
     */
    DATABASE_LEVEL_SYNC("DatabaseLevelSync", "库级别同步，仅对指定的数据库进行数据同步。"),
    /**
     * 表级别同步，只同步指定表的数据
     */
    TABLE_LEVEL_SYNC("TableLevelSync", "表级别同步，仅同步指定的表的数据。");

    private final String levelName;
    private final String description;

    SyncLevel(String levelName, String description) {
        this.levelName = levelName;
        this.description = description;
    }

    /**
     * 获取同步级别的名称
     * @return 同步级别的名称
     */
    public String getLevelName() {
        return levelName;
    }

    /**
     * 获取同步级别的描述信息
     * @return 同步级别的描述信息
     */
    public String getDescription() {
        return description;
    }
}