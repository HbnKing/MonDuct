package com.github.hbnking.buffer;

import com.github.hbnking.model.OplogEntry;
import org.bson.Document;

/**
 * 按文档 ID 分区的策略
 */
public class IdPartitionStrategy implements PartitionStrategy {
    @Override
    public int getPartitionIndex(OplogEntry entry, int disruptorCount) {
        Document fullDocument = entry.getFullDocument();
        if (fullDocument != null && fullDocument.containsKey("_id")) {
            Object id = fullDocument.get("_id");
            return Math.abs(id.hashCode()) % disruptorCount;
        }
        return 0;
    }
}