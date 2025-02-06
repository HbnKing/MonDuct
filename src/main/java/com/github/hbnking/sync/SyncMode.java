package com.github.hbnking.sync;

/**
 * 同步模式枚举，定义了不同的数据同步模式
 */
public enum SyncMode {
    OPLOG,
    CHANGE_STREAM,
    FULL,
    FULL_OPLOG,
    FULL_STREAM
}