package com.github.hbnking.model;

/**
 * @author hbn.king
 * @date 2025/1/31 23:52
 * @description:
 */ // 操作类型枚举，涵盖 Oplog 和 Change Stream 的操作类型
public enum OperationType {
    INSERT, UPDATE, DELETE, CREATE_COLLECTION, DROP_COLLECTION, CREATE_INDEXES, DROP_INDEXES, RENAME_COLLECTION, NOOP,
    MOVE_CHUNK, MOVE_CHUNK_COMPLETED



   /* private final String key;

    public String getKey() {
        return key;
    }

    public String getDesc() {
        return desc;
    }

    private final String desc;


    OperationType(String key,String desc){
        this.key = key ;
        this.desc = desc ;
    }*/
}
