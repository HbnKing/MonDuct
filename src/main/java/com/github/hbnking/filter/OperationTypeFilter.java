package com.github.hbnking.filter;

/**
 * @author hbn.king
 * @date 2025/2/1 12:15
 * @description:
 */


import com.github.hbnking.model.OperationType;
import com.github.hbnking.config.AppConfig;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * 操作类型过滤器，根据配置文件中的操作类型进行过滤
 */
public class OperationTypeFilter {
    private final Set<OperationType> allowedOperationTypes;

    public OperationTypeFilter(AppConfig config) {
        // 假设 AppConfig 中有获取允许的操作类型数组的方法
        //OperationType[] allowedOps = config.getAllowedOperationTypes();
        OperationType[] allowedOps =new OperationType[]{};
        this.allowedOperationTypes = new HashSet<>(Arrays.asList(allowedOps));
    }

    /**
     * 检查操作类型是否允许同步
     * @param operationType 操作类型
     * @return 如果允许同步返回 true，否则返回 false
     */
    public boolean isOperationTypeAllowed(OperationType operationType) {
        return allowedOperationTypes.contains(operationType);
    }
}
