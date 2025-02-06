package com.github.hbnking.monitor;

/**
 * @author hbn.king
 * @date 2025/2/1 12:51
 * @description:
 */


import com.github.hbnking.config.AppConfig;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * JMX 监控类，用于启动 JMX 监控任务，定期收集和展示监控指标
 */
public class JMXMonitor {
    private final AppConfig config;
    private final JMXMetricsCollector metricsCollector;
    private final ScheduledExecutorService executorService;

    public JMXMonitor(AppConfig config, JMXMetricsCollector metricsCollector) {
        this.config = config;
        this.metricsCollector = metricsCollector;
        this.executorService = Executors.newScheduledThreadPool(1);
    }

    /**
     * 启动 JMX 监控
     */
    public void startMonitoring() {
        if (config.isMonitorEnabled()) {
            executorService.scheduleAtFixedRate(() -> {
                try {
                    // 获取 MBeanServer 实例
                    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                    // 注册 MBean
                    ObjectName name = new ObjectName("com.github.hbnking.monitor:type=JMXMetricsCollector");
                    if (!mbs.isRegistered(name)) {
                        mbs.registerMBean(metricsCollector, name);
                    }
                    // 收集并打印监控指标
                    metricsCollector.collectMetrics();
                } catch (Exception e) {
                    System.err.println("JMX 监控出错: " + e.getMessage());
                }
            }, 0, config.getMonitorInterval(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 停止 JMX 监控
     */
    public void stopMonitoring() {
        executorService.shutdown();
    }
}
