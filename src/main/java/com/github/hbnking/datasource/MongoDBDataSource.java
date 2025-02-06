package com.github.hbnking.datasource;

import com.github.hbnking.config.AppConfig;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.client.*;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;

import lombok.Getter;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author wh
 * MongoDB  源端数据特征
 *
 * 基本特性获取链接
 * 创建分片
 * 获取一个分片
 *
 */
@Getter
public class MongoDBDataSource {
    private static final Logger logger = LoggerFactory.getLogger(MongoDBDataSource.class);
    private static  final MongoNamespace defaultNamespace = new MongoNamespace("test.test");
    private static  final MongoNamespace oplog = new MongoNamespace("local","oplog.rs");

    private  MongoClient client;





    public MongoDBDataSource(String url) {

        ConnectionString connectionString = new ConnectionString(url);
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .applyToSocketSettings(builder ->
                        builder.connectTimeout(10, TimeUnit.SECONDS)
                                .readTimeout(30, TimeUnit.SECONDS)
                )
                .build();
        try {
            client = MongoClients.create(settings);


            logger.info("Successfully connected to MongoDB: {}", url);
        } catch (Exception e) {
            logger.error("Failed to connect to MongoDB: {}", url, e);
            throw new RuntimeException("Failed to connect to MongoDB", e);
        }
    }

    public MongoDatabase getDatabase(String dbName) {
        return client.getDatabase(dbName);
    }



    public MongoCollection<Document> getCollection(MongoNamespace namespace) {
        return client.getDatabase(namespace.getDatabaseName()).getCollection(namespace.getCollectionName());
    }




    public void close() {
        try {
            client.close();
            logger.info("MongoDB client has been closed.");
        } catch (Exception e) {
            logger.error("Error while closing MongoDB client: ", e);
        }
    }



    public ClusterType getClusterType(){
        ClusterDescription clusterDescription = this.client.getClusterDescription();
        Optional<ClusterType> first = clusterDescription.getServerDescriptions().stream().map(item -> {
            return item.getClusterType();
        }).findFirst();

        return first.get();
    }



}