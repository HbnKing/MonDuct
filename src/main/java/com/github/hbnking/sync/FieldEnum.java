package com.github.hbnking.sync;



/**
 * @author hbn.king
 * @date 2025/2/5 12:27
 * @description:
 */

public enum FieldEnum {
    ID("_id","id字段名称用于识别主键");


    private final String key;

    public String getKey() {
        return key;
    }

    public String getDesc() {
        return desc;
    }

    private final String desc;

    FieldEnum(String key, String desc) {
        this.key = key;
        this.desc = desc;
    }
}
