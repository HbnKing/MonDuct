package com.github.hbnking.filter;

/**
 * @author hbn.king
 * @date 2025/2/1 10:45
 * @description:
 */


import java.util.regex.Pattern;

/**
 * 命名空间过滤器，用于根据配置过滤命名空间，实现了 NameFilter 接口
 */

import com.github.hbnking.config.AppConfig;
import java.util.regex.Pattern;

/**
 * 命名空间过滤器，根据配置文件中的命名空间正则表达式进行过滤
 */
public class NamespaceFilter {
    private final Pattern[] namespacePatterns;

    public NamespaceFilter(AppConfig config) {
        String[] namespaces = config.getNamespaces();
        this.namespacePatterns = new Pattern[namespaces.length];
        for (int i = 0; i < namespaces.length; i++) {
            this.namespacePatterns[i] = Pattern.compile(namespaces[i]);
        }
    }

    /**
     * 检查命名空间是否允许同步
     * @param namespace 命名空间
     * @return 如果允许同步返回 true，否则返回 false
     */
    public boolean isNamespaceAllowed(String namespace) {
        for (Pattern pattern : namespacePatterns) {
            if (pattern.matcher(namespace).matches()) {
                return true;
            }
        }
        return false;
    }
}