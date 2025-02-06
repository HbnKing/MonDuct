package com.github.hbnking.buffer;

import com.github.hbnking.model.OplogEntry;

/**
 * 分区策略接口，定义根据 OplogEntry 确定分区索引的方法
 */
public interface PartitionStrategy {
    /**
     * 根据 OplogEntry 和 Disruptor 数量计算分区索引
     * @param entry Oplog 条目
     * @param disruptorCount Disruptor 的数量
     * @return 分区索引
     */
    int getPartitionIndex(OplogEntry entry, int disruptorCount);
}