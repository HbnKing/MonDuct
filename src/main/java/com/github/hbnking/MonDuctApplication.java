package com.github.hbnking;

import com.github.hbnking.config.AppConfig;
import com.github.hbnking.buffer.DisruptorBuffer;
import com.github.hbnking.buffer.DatabaseNamePartitionStrategy;
import com.github.hbnking.buffer.PartitionStrategy;
import com.github.hbnking.datasource.MongoDBDataSource;
import com.github.hbnking.filter.FilterUtils;
import com.github.hbnking.sync.ChangeStreamSync;
import com.github.hbnking.sync.DisruptorWriter;
import com.github.hbnking.sync.FullSync;
import com.github.hbnking.sync.OplogSync;
import com.github.hbnking.thread.ThreadManager;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.HashSet;

@SpringBootApplication
public class MonDuctApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(MonDuctApplication.class, args);

        // 获取配置实例
        AppConfig appConfig = context.getBean(AppConfig.class);

        // 创建分区策略实例
        PartitionStrategy partitionStrategy = new DatabaseNamePartitionStrategy();

        // 创建 Disruptor 缓冲区实例
        DisruptorBuffer disruptorBuffer = new DisruptorBuffer(appConfig.getBufferSize(), appConfig.getDisruptorCount());

        HashSet  hashSet  =new HashSet();
        // 创建过滤器工具实例
        FilterUtils filterUtils =new FilterUtils(hashSet,hashSet,hashSet,hashSet );



        DisruptorWriter disruptorWriter = new DisruptorWriter(appConfig, disruptorBuffer);

        FullSync fullSync = new FullSync(appConfig,new MongoDBDataSource(appConfig.getSourceUri()),disruptorBuffer,filterUtils,partitionStrategy);


        OplogSync oplogSync = new OplogSync(appConfig,new MongoDBDataSource(appConfig.getSourceUri()),disruptorBuffer,filterUtils,partitionStrategy);

        ChangeStreamSync changeStreamSync = new ChangeStreamSync(appConfig, disruptorBuffer, filterUtils, partitionStrategy);

        // 创建线程管理器实例
        ThreadManager threadManager = new ThreadManager(appConfig,fullSync,null ,null ,null);



        threadManager.startFullSync();



        // 启动不同的同步任务
     /*   threadManager.startDisruptorWriter();
        threadManager.startFullSync();
        threadManager.startOplogSync();
        threadManager.startChangeStreamSync();*/


        // 注册关闭钩子，确保应用程序关闭时能正确释放资源
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            threadManager.shutdown();
            context.close();
        }));
    }
}