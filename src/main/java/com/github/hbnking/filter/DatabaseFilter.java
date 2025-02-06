package com.github.hbnking.filter;

/**
 * @author hbn.king
 * @date 2025/2/1 12:13
 * @description:
 */

import com.github.hbnking.config.AppConfig;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * 数据库过滤器，根据配置文件中的包含和排除数据库列表进行过滤
 */
public class DatabaseFilter {
    private final Set<String> includedDatabases;
    private final Set<String> excludedDatabases;

    public DatabaseFilter(AppConfig config) {
        this.includedDatabases = new HashSet<>(Arrays.asList(config.getIncludeDatabases()));
        this.excludedDatabases = new HashSet<>(Arrays.asList(config.getExcludeDatabases()));
    }

    /**
     * 检查数据库是否允许同步
     * @param databaseName 数据库名称
     * @return 如果允许同步返回 true，否则返回 false
     */
    public boolean isDatabaseAllowed(String databaseName) {
        if (!includedDatabases.isEmpty() && !includedDatabases.contains(databaseName)) {
            return false;
        }
        return !excludedDatabases.contains(databaseName);
    }
}