package com.github.hbnking.verification;

import com.github.hbnking.datasource.MongoDBDataSource;
import com.github.hbnking.model.OplogEntry;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * 数据验证类，用于验证源数据库和目标数据库的数据一致性
 */
public class DataVerifier {
    private final MongoDBDataSource sourceDataSource;
    private final MongoDBDataSource targetDataSource;

    public DataVerifier(MongoDBDataSource sourceDataSource, MongoDBDataSource targetDataSource) {
        this.sourceDataSource = sourceDataSource;
        this.targetDataSource = targetDataSource;
    }

    /**
     * 验证单个 OplogEntry 对应的数据是否一致
     * @param entry Oplog 条目
     * @return 验证结果
     */
    public VerificationResult verify(OplogEntry entry) {
        VerificationResult result = new VerificationResult();
        String namespace = entry.getNs();
        String[] parts = namespace.split("\\.");
        if (parts.length != 2) {
            result.setVerified(false);
            result.setMessage("命名空间格式不正确: " + namespace);
            return result;
        }
        String databaseName = parts[0];
        String collectionName = parts[1];

        MongoCollection<Document> sourceCollection = sourceDataSource.getDatabase(databaseName).getCollection(collectionName);
        MongoCollection<Document> targetCollection = targetDataSource.getDatabase(databaseName).getCollection(collectionName);

        Document sourceDoc = sourceCollection.find(Filters.eq("_id", entry.getFullDocument().get("_id"))).first();
        Document targetDoc = targetCollection.find(Filters.eq("_id", entry.getFullDocument().get("_id"))).first();

        if (sourceDoc == null && targetDoc == null) {
            result.setVerified(true);
            result.setMessage("源文档和目标文档都不存在，视为一致");
        } else if (sourceDoc == null || targetDoc == null) {
            result.setVerified(false);
            result.setMessage("源文档和目标文档存在一方缺失");
        } else if (sourceDoc.equals(targetDoc)) {
            result.setVerified(true);
            result.setMessage("源文档和目标文档一致");
        } else {
            result.setVerified(false);
            result.setMessage("源文档和目标文档不一致");
            result.addDetail("sourceDoc", sourceDoc);
            result.addDetail("targetDoc", targetDoc);
        }

        return result;
    }

    /**
     * 验证指定集合的数据一致性
     * @param databaseName 数据库名称
     * @param collectionName 集合名称
     * @return 验证结果列表
     */
    public List<VerificationResult> verifyCollection(String databaseName, String collectionName) {
        List<VerificationResult> results = new ArrayList<>();
        MongoCollection<Document> sourceCollection = sourceDataSource.getDatabase(databaseName).getCollection(collectionName);
        MongoCursor<Document> cursor = sourceCollection.find().iterator();
        while (cursor.hasNext()) {
            Document sourceDoc = cursor.next();
            OplogEntry mockEntry = new OplogEntry();
            mockEntry.setFullDocument(sourceDoc);
            mockEntry.setNs(databaseName + "." + collectionName);
            results.add(verify(mockEntry));
        }
        return results;
    }
}