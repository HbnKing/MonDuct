# MongoDB 数据同步项目说明文档

## 一、项目概述
本项目旨在实现 MongoDB 数据库之间的数据同步功能，支持全量同步、Oplog 同步以及 Change Stream 同步三种模式。通过多线程和 Disruptor 技术提高同步效率，同时提供灵活的过滤策略和配置选项，以满足不同场景下的数据同步需求。

## 二、项目结构

### 2.1 整体结构
```plaintext
com.github.hbnking
├── config
│   └── AppConfig.java
├── datasource
│   └── MongoDBDataSource.java
├── filter
│   ├── FilterConfig.java
│   ├── FilterUtils.java
│   ├── BaseFilterStrategy.java
│   ├── DatabaseFilterStrategy.java
│   └── TableFilterStrategy.java
├── model
│   ├── IndexInfo.java
│   ├── OplogEntry.java
│   └── OperationType.java
├── buffer
│   ├── DisruptorBuffer.java
│   └── PartitionStrategy.java
├── sync
│   ├── ChangeStreamSync.java
│   ├── DisruptorWriter.java
│   ├── FullSync.java
│   └── OplogSync.java
├── thread
│   └── ThreadManager.java
├── controller
│   └── SyncController.java
└── MonDuctApplication.java
```

### 2.2 各模块说明

#### 2.2.1 config 模块
包含 `AppConfig` 类，负责从配置文件中读取 MongoDB 同步相关的配置信息，如同步模式、源和目标数据库 URI、线程数量等，并提供默认值。

#### 2.2.2 datasource 模块
`MongoDBDataSource` 类用于建立与 MongoDB 数据库的连接，提供获取数据库和集合的方法，同时处理资源关闭和异常情况。

#### 2.2.3 filter 模块
- `FilterConfig`：存储过滤配置信息，如包含和排除的数据库、表等。
- `FilterUtils`：根据配置信息对 `OplogEntry` 进行过滤。
- `BaseFilterStrategy`：抽象类，定义过滤策略的基本接口。
- `DatabaseFilterStrategy` 和 `TableFilterStrategy`：分别实现对数据库和表的过滤逻辑。

#### 2.2.4 model 模块
- `IndexInfo`：存储索引相关信息。
- `OplogEntry`：表示 Oplog 或 Change Stream 条目的实体类，包含从 BSON 文档解析的静态方法。
- `OperationType`：定义操作类型的枚举类，如插入、更新、删除等。

#### 2.2.5 buffer 模块
- `DisruptorBuffer`：管理 `Disruptor` 实例，用于存储和传递 `OplogEntry` 事件。
- `PartitionStrategy`：确定 `OplogEntry` 事件的分区策略。

#### 2.2.6 sync 模块
- `ChangeStreamSync`：处理 MongoDB 的 Change Stream 同步任务，将事件转换为 `OplogEntry` 并发布到 `Disruptor` 缓冲区。
- `DisruptorWriter`：从 `Disruptor` 中获取 `OplogEntry` 事件，并将数据写入目标 MongoDB 数据库。
- `FullSync`：负责全量同步任务。
- `OplogSync`：处理 MongoDB 的 Oplog 同步任务。

#### 2.2.7 thread 模块
`ThreadManager` 类管理同步任务的线程调度，包括启动不同类型的同步任务和 `DisruptorWriter`，并提供关闭资源的方法。

#### 2.2.8 controller 模块
`SyncController` 类作为控制层，提供 RESTful 接口，用于启动和停止同步任务。

#### 2.2.9 主类
`MonDuctApplication` 是项目的启动类，用于启动整个 MongoDB 同步应用程序。

## 三、功能特性

### 3.1 同步模式
- **全量同步**：将源数据库中的所有数据复制到目标数据库。
- **Oplog 同步**：实时同步源数据库的操作日志（Oplog）到目标数据库，确保数据的实时性。
- **Change Stream 同步**：利用 MongoDB 的 Change Stream 功能，监听源数据库的变更事件并同步到目标数据库。

### 3.2 过滤策略
支持通过配置包含和排除的数据库、表来过滤同步的数据，提高同步的精准性和效率。

### 3.3 多线程和 Disruptor 技术
采用多线程和 Disruptor 技术，提高数据处理和同步的效率，减少数据同步的延迟。

### 3.4 可配置性
通过 `AppConfig` 类，可以灵活配置同步模式、线程数量、缓冲区大小、监控间隔等参数，以适应不同的业务需求。

## 四、使用说明

### 4.1 配置文件
在 `application.properties` 或 `application.yml` 中配置 MongoDB 同步的相关参数，示例如下：
```properties
mongodb.sync.syncMode=full
mongodb.sync.sourceUri=mongodb://source_host:port
mongodb.sync.targetUri=mongodb://target_host:port
mongodb.sync.bufferSize=10000
mongodb.sync.readThreads=4
mongodb.sync.writeThreads=4
mongodb.sync.shouldSyncIndexes=true
mongodb.sync.monitorEnabled=true
mongodb.sync.monitorInterval=5000
mongodb.sync.delaySync=0
mongodb.sync.syncDdl=false
mongodb.sync.syncIndexAfter60Percent=true
mongodb.sync.multiTableParallel=true
mongodb.sync.disruptorCount=8
mongodb.sync.fullSync=true
mongodb.sync.includeDatabases=db1,db2
mongodb.sync.excludeDatabases=
mongodb.sync.includeTables=table1,table2
mongodb.sync.excludeTables=
mongodb.sync.namespaces=
mongodb.sync.regexNamespaces=
mongodb.sync.maxRetries=3
mongodb.sync.retryDelay=2000
mongodb.sync.dataVerificationEnabled=false
mongodb.sync.dataVerificationInterval=60000
```

### 4.2 启动项目
运行 `MonDuctApplication` 类的 `main` 方法启动项目。

### 4.3 控制同步任务
可以通过调用 `SyncController` 提供的 RESTful 接口来启动和停止同步任务：
- 启动同步：`GET http://localhost:8080/sync/start`
- 停止同步：`GET http://localhost:8080/sync/stop`

## 五、代码逻辑详解

### 5.1 数据解析
`OplogEntry` 类中的 `fromDocument` 方法根据文档类型（Oplog 或 Change Stream）调用不同的解析方法（`fromOplog` 或 `fromChangeStream`），将 BSON 文档解析为 `OplogEntry` 对象。

### 5.2 过滤逻辑
`FilterUtils` 类结合 `DatabaseFilterStrategy` 和 `TableFilterStrategy`，根据配置的包含和排除规则对 `OplogEntry` 进行过滤，决定是否处理该条目。

### 5.3 同步任务
- `ThreadManager` 类负责管理和调度不同的同步任务，包括全量同步、Oplog 同步、Change Stream 同步和 `DisruptorWriter` 任务。
- `ChangeStreamSync` 和 `OplogSync` 类分别监听 Change Stream 和 Oplog 的变更事件，将事件转换为 `OplogEntry` 并发布到 `Disruptor` 缓冲区。
- `DisruptorWriter` 类从 `Disruptor` 缓冲区中获取 `OplogEntry` 事件，并将数据写入目标 MongoDB 数据库。

### 5.4 控制层
`SyncController` 类提供 RESTful 接口，通过调用 `ThreadManager` 的相应方法来启动和停止同步任务。

## 六、注意事项
- 确保源和目标 MongoDB 数据库的连接信息正确，并且具有相应的读写权限。
- 在配置过滤规则时，注意数据库名和表名的大小写，MongoDB 是区分大小写的。
- 对于大规模数据同步，可根据实际情况调整线程数量、缓冲区大小等参数，以优化同步性能。

## 七、未来改进方向
- 增加数据验证和冲突解决机制，确保数据的一致性和完整性。
- 支持更多的同步模式和过滤规则，如按时间范围同步、按字段值过滤等。
- 提供更详细的监控和日志信息，方便问题排查和性能优化。