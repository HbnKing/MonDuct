package com.github.hbnking.buffer;

import com.github.hbnking.model.OplogEntry;

/**
 * 按数据库名和表名组合取模的分区策略
 */
public class DatabaseTablePartitionStrategy implements PartitionStrategy {
    @Override
    public int getPartitionIndex(OplogEntry entry, int disruptorCount) {
        String namespace = entry.getNs();
        return Math.abs(namespace.hashCode()) % disruptorCount;
    }
}