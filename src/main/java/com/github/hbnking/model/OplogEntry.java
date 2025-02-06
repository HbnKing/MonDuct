package com.github.hbnking.model;

import com.github.hbnking.sync.FieldEnum;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import lombok.Data;
import org.bson.Document;
import java.util.*;


// Oplog 或 Change Stream 条目类
@Data
public class OplogEntry {
    // 静态映射，用于快速查找操作类型
    private static final Map<String, OperationType> OPERATION_TYPE_MAP = new HashMap<>();
    static {
        for (OperationType type : OperationType.values()) {
            OPERATION_TYPE_MAP.put(type.name(), type);
        }
    }

    private OperationType op;
    private String ns;
    private UUID ui;
    private Document o;
    private Document o2;
    private long numRecords;
    private Object ts;
    private long t;
    private long v;
    private Object wall;
    private OperationType command;
    private String collectionName;
    private List<IndexInfo> indexes;
    private String fromShard;
    private String toShard;
    private Document idIndex;
    private Document collation;
    private Document fullDocument;
    private Document documentKey;
    private String from;



    /**
     * @param doc
     * @param namespace
     * @return
     */
    public static OplogEntry fromOriginal(Document doc, MongoNamespace namespace) {
        OplogEntry entry = new OplogEntry();

        entry.setFullDocument(doc);
        entry.setOp(OperationType.INSERT);
        entry.setNs(namespace.getFullName());


        return entry ;
    }

    // 从 Change Stream 文档解析 OplogEntry
    public static OplogEntry fromChangeStream(ChangeStreamDocument doc) {
        OplogEntry entry = new OplogEntry();
        String operationTypeStr = doc.getOperationTypeString();
        switch (operationTypeStr) {
            case "insert":
                entry.setOp(OperationType.INSERT);
                break;
            case "update":
                entry.setOp(OperationType.UPDATE);
                break;
            case "delete":
                entry.setOp(OperationType.DELETE);
                break;
            default:
                throw new IllegalArgumentException("Unsupported Change Streams operation type: " + operationTypeStr);
        }
       /* entry.setTs(doc.get("clusterTime"));
        Document nsDoc = doc.get("ns", Document.class);
        entry.setNs(nsDoc.getString("db") + "." + nsDoc.getString("coll"));
        entry.setFullDocument(doc.get("fullDocument", Document.class));
        entry.setDocumentKey(doc.get("documentKey", Document.class));
        if (doc.containsKey("from")) {
            entry.setFrom(doc.getString("from"));
        }*/
        return entry;
    }

    // 从 Oplog 文档解析 OplogEntry
    public static OplogEntry fromOplog(Document doc) {
        OplogEntry entry = new OplogEntry();
        String opStr = doc.getString("op").toUpperCase();
        entry.setOp(OPERATION_TYPE_MAP.get(opStr));
        entry.setNs(doc.getString("ns"));

        if (doc.containsKey("ui")) {
            String uuidStr = doc.getString("ui");
            if (uuidStr.startsWith("UUID(")) {
                uuidStr = uuidStr.replace("UUID(", "").replace(")", "");
            }
            entry.setUi(UUID.fromString(uuidStr));
        }

        entry.setO(doc.get("o", Document.class));
        if (doc.containsKey("o2")) {
            entry.setO2(doc.get("o2", Document.class));
            if (entry.getO2().containsKey("numRecords")) {
                entry.setNumRecords(entry.getO2().getLong("numRecords"));
            }
        }
        entry.setTs(doc.get("ts"));
        entry.setT(doc.getLong("t"));
        entry.setV(doc.getLong("v"));
        entry.setWall(doc.get("wall"));



        // 根据操作类型设置具体的命令和相关信息
        switch (entry.getOp()) {
            case INSERT:
                entry.setCommand(OperationType.INSERT);
                if ("system.indexes".equals(entry.getNs().split("\\.")[1])) {
                    entry.setCommand(OperationType.CREATE_INDEXES);
                    entry.setCollectionName(entry.getO().getString("ns").split("\\.")[1]);
                    List<IndexInfo> indexInfos = new ArrayList<>();
                    indexInfos.add(new IndexInfo(entry.getO()));
                    entry.setIndexes(indexInfos);
                }
                break;
            case UPDATE:
                entry.setCommand(OperationType.UPDATE);
                // 处理 diff 形式的更新
                if (entry.getO().containsKey("diff")) {
                    // 这里可以添加具体的 diff 解析逻辑
                }
                break;
            case DELETE:
                entry.setCommand(OperationType.DELETE);
                break;
            case CREATE_COLLECTION:
                entry.setCommand(OperationType.CREATE_COLLECTION);
                if (entry.getO().containsKey("create")) {
                    entry.setCollectionName(entry.getO().getString("create"));
                    if (entry.getO().containsKey("idIndex")) {
                        entry.setIdIndex(entry.getO().get("idIndex", Document.class));
                    }
                    if (entry.getO().containsKey("collation")) {
                        entry.setCollation(entry.getO().get("collation", Document.class));
                    }
                }
                break;
            case DROP_COLLECTION:
                entry.setCommand(OperationType.DROP_COLLECTION);
                if (entry.getO().containsKey("drop")) {
                    entry.setCollectionName(entry.getO().getString("drop"));
                }
                break;
            case CREATE_INDEXES:
                entry.setCommand(OperationType.CREATE_INDEXES);
                if (entry.getO().containsKey("createIndexes")) {
                    entry.setCollectionName(entry.getO().getString("createIndexes"));
                    List<IndexInfo> indexInfos = new ArrayList<>();
                    if (entry.getO().containsKey("indexes")) {
                        List<Document> indexDocs = (List<Document>) entry.getO().get("indexes");
                        for (Document indexDoc : indexDocs) {
                            indexInfos.add(new IndexInfo(indexDoc));
                        }
                    } else {
                        indexInfos.add(new IndexInfo(entry.getO()));
                    }
                    entry.setIndexes(indexInfos);
                }
                break;
            case DROP_INDEXES:
                entry.setCommand(OperationType.DROP_INDEXES);
                if (entry.getO().containsKey("dropIndexes")) {
                    entry.setCollectionName(entry.getO().getString("dropIndexes"));
                }
                break;
            case RENAME_COLLECTION:
                entry.setCommand(OperationType.RENAME_COLLECTION);
                if (entry.getO().containsKey("renameCollection")) {
                    String fromCollection = entry.getO().getString("renameCollection");
                    String toCollection = entry.getO().getString("to");
                    entry.setCollectionName(fromCollection);
                    // 这里可以根据需求进一步处理重命名信息
                }
                break;
            case NOOP:
                entry.setCommand(OperationType.NOOP);
                break;
            case MOVE_CHUNK:
                entry.setCommand(OperationType.MOVE_CHUNK);
                if (entry.getO().containsKey("moveChunk")) {
                    entry.setCollectionName(entry.getO().getString("moveChunk").split("\\.")[1]);
                    entry.setFromShard(entry.getO().getString("from"));
                    entry.setToShard(entry.getO().getString("to"));
                }
                break;
            case MOVE_CHUNK_COMPLETED:
                entry.setCommand(OperationType.MOVE_CHUNK_COMPLETED);
                if (entry.getO().containsKey("moveChunkCompleted")) {
                    entry.setCollectionName(entry.getO().getString("moveChunkCompleted").split("\\.")[1]);
                    entry.setFromShard(entry.getO().getString("from"));
                    entry.setToShard(entry.getO().getString("to"));
                }
                break;
        }
        return entry;
    }

    // Getter 和 Setter 方法
    public OperationType getOp() {
        return op;
    }

    public void setOp(OperationType op) {
        this.op = op;
    }

    public String getNs() {
        return ns;
    }

    public void setNs(String ns) {
        this.ns = ns;
    }

    public UUID getUi() {
        return ui;
    }

    public void setUi(UUID ui) {
        this.ui = ui;
    }

    public Document getO() {
        return o;
    }

    public void setO(Document o) {
        this.o = o;
    }

    public Document getO2() {
        return o2;
    }

    public void setO2(Document o2) {
        this.o2 = o2;
    }

    public long getNumRecords() {
        return numRecords;
    }

    public void setNumRecords(long numRecords) {
        this.numRecords = numRecords;
    }

    public Object getTs() {
        return ts;
    }

    public void setTs(Object ts) {
        this.ts = ts;
    }

    public long getT() {
        return t;
    }

    public void setT(long t) {
        this.t = t;
    }

    public long getV() {
        return v;
    }

    public void setV(long v) {
        this.v = v;
    }

    public Object getWall() {
        return wall;
    }

    public void setWall(Object wall) {
        this.wall = wall;
    }

    public OperationType getCommand() {
        return command;
    }

    public void setCommand(OperationType command) {
        this.command = command;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public List<IndexInfo> getIndexes() {
        return indexes;
    }

    public void setIndexes(List<IndexInfo> indexes) {
        this.indexes = indexes;
    }

    public String getFromShard() {
        return fromShard;
    }

    public void setFromShard(String fromShard) {
        this.fromShard = fromShard;
    }

    public String getToShard() {
        return toShard;
    }

    public void setToShard(String toShard) {
        this.toShard = toShard;
    }

    public Document getIdIndex() {
        return idIndex;
    }

    public void setIdIndex(Document idIndex) {
        this.idIndex = idIndex;
    }

    public Document getCollation() {
        return collation;
    }

    public void setCollation(Document collation) {
        this.collation = collation;
    }

    public Document
    getFullDocument() {
        return fullDocument;
    }

    public void setFullDocument(Document fullDocument) {
        this.fullDocument = fullDocument;
    }

    public Document getDocumentKey() {
        return documentKey;
    }

    public void setDocumentKey(Document documentKey) {
        this.documentKey = documentKey;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    @Override
    public String toString() {
        return "OplogEntry{" +
                "op=" + op +
                ", ns='" + ns + '\'' +
                ", ui=" + ui +
                ", o=" + o +
                ", o2=" + o2 +
                ", numRecords=" + numRecords +
                ", ts=" + ts +
                ", t=" + t +
                ", v=" + v +
                ", wall=" + wall +
                ", command=" + command +
                ", collectionName='";}}