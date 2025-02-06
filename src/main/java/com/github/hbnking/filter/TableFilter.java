package com.github.hbnking.filter;

/**
 * @author hbn.king
 * @date 2025/2/1 12:14
 * @description:
 */


import com.github.hbnking.config.AppConfig;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * 表过滤器，根据配置文件中的包含和排除表列表进行过滤
 */
public class TableFilter {
    private final Set<String> includedTables;
    private final Set<String> excludedTables;

    public TableFilter(AppConfig config) {
        this.includedTables = new HashSet<>(Arrays.asList(config.getIncludeTables()));
        this.excludedTables = new HashSet<>(Arrays.asList(config.getExcludeTables()));
    }

    /**
     * 检查表是否允许同步
     * @param tableName 表名称
     * @return 如果允许同步返回 true，否则返回 false
     */
    public boolean isTableAllowed(String tableName) {
        if (!includedTables.isEmpty() && !includedTables.contains(tableName)) {
            return false;
        }
        return !excludedTables.contains(tableName);
    }
}
