
package com.github.hbnking.thread.status;


import lombok.extern.log4j.Log4j2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @desc: 程序状态符
 * @author: lhp
 * @time: 2021/12/1 10:09 上午
 */
@Log4j2
public class WorkStatus {
    /**
     * 程序关闭
     */
    public static final int WORK_STOP = -1;
    /**
     * 程序运行
     */
    public static final int WORK_RUN = 1;
    /**
     * 程序暂停状态
     */
    public static final int WORK_PAUSE = 2;

    /**
     * 存储每个工作名称的工作状态的映射。
     * k为程序名
     * v为状态
     */
    private static final Map<String, Integer> WORK_STATUS_MAP = new ConcurrentHashMap<>();

    /**
     * 移除指定工作名称的工作状态。
     *
     * @param workName 工作名称。
     */
    public static void removeWorkStatus(String workName) {
        WORK_STATUS_MAP.remove(workName);
    }

    /**
     * 更新指定工作名称的工作状态。
     *
     * @param workName 工作名称。
     * @param status   更新后的工作状态。
     */
    public static void updateWorkStatus(String workName, int status) {
        WORK_STATUS_MAP.put(workName, status);
    }

    /**
     * 获取指定工作名称的工作状态。
     * 如果未找到工作状态，则返回默认状态。
     *
     * @param workName 工作名称。
     * @return 工作状态。
     */
    public static int getWorkStatus(String workName) {
        return WORK_STATUS_MAP.getOrDefault(workName, WORK_STOP);
    }


}
