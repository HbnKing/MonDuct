package com.github.hbnking.monitor;

/**
 * @author hbn.king
 * @date 2025/2/1 12:52
 * @description:
 */

import com.github.hbnking.buffer.DisruptorBuffer;
import com.github.hbnking.service.SyncService;

import java.util.concurrent.atomic.AtomicLong;

/**
 * JMX 指标收集器，用于收集和管理系统的监控指标
 */
public class JMXMetricsCollector implements JMXMetricsCollectorMBean {
    private final DisruptorBuffer disruptorBuffer;
    private final SyncService syncService;
    private final AtomicLong totalProcessedEvents = new AtomicLong(0);

    public JMXMetricsCollector(DisruptorBuffer disruptorBuffer, SyncService syncService) {
        this.disruptorBuffer = disruptorBuffer;
        this.syncService = syncService;
    }

    /**
     * 收集监控指标
     */
    public void collectMetrics() {
        long bufferSize = disruptorBuffer.getDisruptors().size();
        long processedEvents = syncService.getProcessedEventsCount();
        totalProcessedEvents.addAndGet(processedEvents);

        System.out.println("Disruptor 缓冲区大小: " + bufferSize);
        System.out.println("本次处理的事件数量: " + processedEvents);
        System.out.println("总共处理的事件数量: " + totalProcessedEvents.get());
    }

    @Override
    public long getTotalProcessedEvents() {
        return totalProcessedEvents.get();
    }
}

/**
 * JMXMetricsCollector 的 MBean 接口
 */
interface JMXMetricsCollectorMBean {
    long getTotalProcessedEvents();
}