package com.github.hbnking.filter;

import com.github.hbnking.model.OplogEntry;
import java.util.Set;

public class FilterUtils {
    private final Set<String> includeDatabases;
    private final Set<String> excludeDatabases;
    private final Set<String> includeTables;
    private final Set<String> excludeTables;

    public FilterUtils(Set<String> includeDatabases, Set<String> excludeDatabases,
                       Set<String> includeTables, Set<String> excludeTables) {
        this.includeDatabases = includeDatabases;
        this.excludeDatabases = excludeDatabases;
        this.includeTables = includeTables;
        this.excludeTables = excludeTables;
    }

    /**
     * 判断是否应该处理给定的 OplogEntry
     * @param oplogEntry 要判断的 OplogEntry 对象
     * @return 如果应该处理返回 true，否则返回 false
     */
    public boolean shouldProcess(OplogEntry oplogEntry) {
        String namespace = oplogEntry.getNs();
        if (namespace == null) {
            return false;
        }
        String[] parts = namespace.split("\\.");
        if (parts.length != 2) {
            return false;
        }
        String database = parts[0];
        String table = parts[1];

        // 检查是否在排除的数据库列表中
        if (excludeDatabases.contains(database)) {
            return false;
        }
        // 检查是否在排除的表列表中
        if (excludeTables.contains(table)) {
            return false;
        }
        // 如果有包含的数据库列表，检查是否在其中
        if (!includeDatabases.isEmpty() && !includeDatabases.contains(database)) {
            return false;
        }
        // 如果有包含的表列表，检查是否在其中
        if (!includeTables.isEmpty() && !includeTables.contains(table)) {
            return false;
        }
        return true;
    }
}