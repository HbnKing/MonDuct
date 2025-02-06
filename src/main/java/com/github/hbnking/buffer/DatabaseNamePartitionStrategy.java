package com.github.hbnking.buffer;

import com.github.hbnking.model.OplogEntry;

/**
 * 按数据库名取模的分区策略
 */
public class DatabaseNamePartitionStrategy implements PartitionStrategy {
    @Override
    public int getPartitionIndex(OplogEntry entry, int disruptorCount) {
        String namespace = entry.getNs();
        String databaseName = namespace.split("\\.")[0];
        return Math.abs(databaseName.hashCode()) % disruptorCount;
    }
}