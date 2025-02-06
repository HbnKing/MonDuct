package com.github.hbnking.datasource;


import com.mongodb.BasicDBObject;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;


import com.mongodb.client.MongoIterable;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.bson.BsonType;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;


@Log4j2
@Getter
@Setter
public class SpliceNsData {

    private SpliceNsData(){}


    private static  SpliceNsData  spliceNsData =null;

    /**
     * 数据源名称
     */
    private  String dsName;
    /**
     * mongoClient
     */
    private static MongoClient mongoClient;
    /**
     * 一个任务读取大小MB
     * 默认 为32mb
     */
    private static int mbSize = 32;


    private Set<NameSpaceRange> namespaces  = new ConcurrentSkipListSet<>();
    private String currentdatabaseName;
    private Set<String> databases ;


    public static SpliceNsData init(MongoClient mongoClient){

        if(spliceNsData !=null){
            return spliceNsData ;
        }
        spliceNsData =  new SpliceNsData(mongoClient) ;

        return spliceNsData ;

    }



    /**
     * 构造函数，初始化数据源名称和任务读取大小
     *
     *
     */
    private SpliceNsData(MongoClient mongoClient) {
        this.mongoClient = mongoClient;

        ArrayList<String> dbs = mongoClient.listDatabaseNames().into(new ArrayList<>());

        this.databases = new HashSet<>(dbs);
    }

    /**
     * 获取某表中的主键类型的最大和最小值
     *
     * @param mongoNamespace 库表名
     * @return Map<Integer, Range>
     * 键：主键类型
     * 值：该类型主键的最大和最小值
     * @desc 获取某表中的主键类型的最大和最小值
     */
    private Map<Integer, Range> getIdTypes(MongoNamespace mongoNamespace ) {
        Map<Integer, Range> typeMap = new HashMap<>();
        
        BasicDBObject basicDBObject = new BasicDBObject();
        for (BsonType next :BsonType.values()) {
            int type = next.getValue();
            // 过滤不可能为主键数据的类型
            if (type == 4 ) {
               continue;
            }
            // 当在大表中 查询_id类型为4（array），会出现卡死
            try {
                basicDBObject.append("_id", new Document().append("$type", type));
                Document document = mongoClient.getDatabase(mongoNamespace.getDatabaseName()).getCollection(mongoNamespace.getCollectionName()).find(basicDBObject)
                        .projection(new BasicDBObject().append("_id", 1)).first();
                // 判断某类型的主键是否有数据
                if (document != null) {
                    log.info("{} {} the _id type of the table is:{}", dsName, mongoNamespace, next.name());
                    Range range = getMaxAndMinIdByNs(mongoNamespace, type);
                    typeMap.put(type, range);
                }
            } catch (Exception e) {
                log.error("{} an error occurred when splitting the {} table task, and the error message was reported:{}", dsName, mongoNamespace, e.getMessage());
            }
        }
        return typeMap;
    }

    /**
     * 获取某类型主键的最大和最小值
     *
     * @param mongoNamespace   库表名
     * @param type 数据类型
     * @return Range
     * 某类型主键的最大和最小值
     * @desc 获取某类型主键的最大和最小值
     */
    private Range getMaxAndMinIdByNs(MongoNamespace mongoNamespace , int type) {
        Object maxId = Integer.MAX_VALUE;
        Object minId = Integer.MIN_VALUE;
        boolean isSuccessComputeMaxAndMin = false;
        // 循环三次查询
        for (int index = 1; index <= 3; index++) {
            try {
                
                BasicDBObject condition = new BasicDBObject();
                condition.append("_id", new Document().append("$type", type));
                BasicDBObject sort = new BasicDBObject();
                sort.append("_id", -1);
                Document maxDocument = mongoClient.getDatabase(mongoNamespace.getDatabaseName())
                        .getCollection(mongoNamespace.getCollectionName()).find(condition)
                        .sort(sort).first();
                sort.append("_id", 1);
                Document minDocument = mongoClient.getDatabase(mongoNamespace.getDatabaseName())
                        .getCollection(mongoNamespace.getCollectionName()).find(condition)
                        .sort(sort).first();
                maxId = maxDocument.get("_id");
                minId = minDocument.get("_id");
                isSuccessComputeMaxAndMin = true;
                // 此某类型数据range的范围
            } catch (Exception e) {
                log.error("{} an error occurred when calculating the maximum and minimum values of [{}] table _id, and an error message was reported:{}", dsName, mongoNamespace, e.getMessage());
                isSuccessComputeMaxAndMin = false;
            }
            if (isSuccessComputeMaxAndMin) {
                break;
            } else {
                if (index == 3) {
                    log.error("{} an error occurred when calculating the maximum and minimum values of [{}] table _id, and the error message was reported: Failed to read three times, please check the source URL and permissions", dsName, mongoNamespace);
                }
            }
        }
        Range range = new Range();
        range.setColumnName("_id");
        range.setMaxValue(maxId);
        range.setMinValue(minId);
        range.setNs(mongoNamespace.getFullName());
        return range;
    }

    /**
     * 切分数据，每分数据最大长度为50w
     *
     * @param mongoNamespace           库表名
     * @param rangeOfTable 表范围range
     * @param type         数据类型
     * @param rangeSize    切分数据的最大长度
     * @return Range
     * 某个区间的range
     * @desc 切分数据，每分数据最大长度为50w
     */
    private Range splitRange(MongoNamespace mongoNamespace , Range rangeOfTable, int type, int rangeSize) {
        Range range = new Range();

        range.setColumnName(rangeOfTable.getColumnName());
        
        BasicDBObject condition = new BasicDBObject();
        // 不要紧在where条件中单独添加type的查询
        condition.append(rangeOfTable.getColumnName(), new Document("$gte", rangeOfTable.getMinValue()));
        Document document = null;
        // 循环三次查询
        boolean isSuccessComputeMaxAndMin = false;
        for (int index = 1; index <= 3; index++) {
            try {
                document = mongoClient.getDatabase(mongoNamespace.getDatabaseName()).getCollection(mongoNamespace.getCollectionName()).
                        find(condition).sort(new Document("_id", 1)).projection(new Document("_id", 1)).skip(rangeSize).first();
                isSuccessComputeMaxAndMin = true;
            } catch (Exception e) {
                log.error("{} [{}] condition:[{}],error message was reported:{}", dsName, mongoNamespace, condition.toJson(), e.getMessage());
                isSuccessComputeMaxAndMin = false;
            }
            if (isSuccessComputeMaxAndMin) {
                break;
            } else {
                if (index == 3) {
                    log.error("{} [{}] condition:[{}],,error message was reported::Failed to split three times", dsName, mongoNamespace, condition.toJson());
                }
            }
        }
        // 如果当前minId的后xxw条的_id字段为空，说明达到该类型数据的最大值
        if (document != null) {
            Object maxIdRTemp = document.get("_id");
            range.setMinValue(rangeOfTable.getMinValue());
            range.setMaxValue(maxIdRTemp);
            range.setNs(mongoNamespace.getFullName());
            rangeOfTable.setMinValue(maxIdRTemp);
        } else {
            range.setMinValue(rangeOfTable.getMinValue());
            range.setMaxValue(rangeOfTable.getMaxValue());
            range.setNs(mongoNamespace.getFullName());
            range.setMax(true);
            //要修改总的rangeOfTable的范围
            rangeOfTable.setMinValue(null);
        }
        range.setNs(mongoNamespace.getFullName());
        range.setRangeSize(rangeSize);
        return range;
    }

    /**
     * 估算库表的文档数量
     *
     * @param mongoNamespace 库表名
     * @return long
     * 估算的文档数量
     */
    private long estimateRangeSize(MongoNamespace mongoNamespace ) {
        
        return mongoClient.getDatabase(mongoNamespace.getDatabaseName()).getCollection(mongoNamespace.getCollectionName()).estimatedDocumentCount();
    }

    /**
     * 计算每次查询的批处理大小
     *
     * @param mongoNamespace 库表名
     * @return int
     * 计算得到的批处理大小
     */
    private int computeBatchSize(MongoNamespace mongoNamespace ) {
       
        // 查询改表的collStats
        Document collStats = mongoClient.getDatabase(mongoNamespace.getDatabaseName()).runCommand(new Document("collStats", mongoNamespace.getCollectionName()));
        collStats.remove("wiredTiger");
        log.info("{} ns collection status information:{}", dsName, collStats.toJson());

        // 可以任务没有数据
        if (!collStats.containsKey("avgObjSize")) {
            return 10240;
        }
        // 每条数据的byte数
        long avgObjSize = Long.parseLong(collStats.get("avgObjSize").toString());

        try {
            // 最大一批数据 一百万一批数据
            int batchSize = Math.round(mbSize * 1024L * 1024L / (avgObjSize + 0.0F));
            // 100万
            if (batchSize >= 1024000) {
                return 1024000;
            }
            // 1千
            if (batchSize <= 1024) {
                return 1024;
            }
            return batchSize;
        } catch (Exception e) {
            return 10240;
        }
    }

    /**
     * 获取库表每条数据的avg大小
     *
     * @param mongoNamespace 库表名
     * @return int
     */
    private long getAvgObjSize(MongoNamespace mongoNamespace ) {
        
        // 查询改表的collStats
        Document collStats = mongoClient.getDatabase(mongoNamespace.getDatabaseName()).runCommand(new Document("collStats", mongoNamespace.getCollectionName()));
        collStats.remove("wiredTiger");
        // 压缩前的大小
        if (!collStats.containsKey("avgObjSize")) {
            return 1024;
        }
        // 每条数据的byte数
        long avgObjSize = Long.parseLong(collStats.get("avgObjSize").toString());
        return avgObjSize;
    }

    /**
     * 获取库表的范围列表
     *
     * @param mongoNamespace 库表名
     * @return List<Range>
     * 库表的范围列表
     */
    public List<NameSpaceRange> getRangeList(MongoNamespace mongoNamespace) {
        List<NameSpaceRange> rangeList = new ArrayList<>();
       
        long count = mongoClient.getDatabase(mongoNamespace.getDatabaseName()).getCollection(mongoNamespace.getCollectionName()).estimatedDocumentCount();
        int batchSize = computeBatchSize(mongoNamespace);
        final long avgObjSize = getAvgObjSize(mongoNamespace);
        log.info("{} estimated amount of data in ns :{}, each batch of data is expected to {}", dsName, count, batchSize);
        if (count > 0) {
            Map<Integer, Range> map = getIdTypes(mongoNamespace);
            for (Map.Entry<Integer, Range> next : map.entrySet()) {
                Range rangeOfTable = next.getValue();
                while (rangeOfTable.getMinValue() != null) {
                    Range range = splitRange(mongoNamespace, rangeOfTable, next.getKey(), batchSize);
                    // 设置文档大小
                    range.setAvgObjSize(avgObjSize);

                    rangeList.add(new NameSpaceRange(range,mongoNamespace));
                }
            }
        }
        return rangeList;
    }


    public boolean hasNext(){

        if(nameSpacehasNext()){
            return true ;
        }

        fillNameSpace();


        return nameSpacehasNext();

    }

    private  boolean  nameSpacehasNext(){
        Optional<NameSpaceRange> namespace = namespaces.stream().filter(item -> {
            return !item.getRange().getStatus().get();
        }).findFirst();

        if(namespace.isPresent()){
            return true ;
        }else {
            return false ;
        }
    }


    public NameSpaceRange  getNext(){

        Optional<NameSpaceRange> namespace = namespaces.stream().filter(item -> {
            return !item.getRange().getStatus().get();
        }).findFirst();

        if(namespace.isPresent()){
            boolean b = namespace.get().getRange().getStatus().compareAndSet(false, true);

            return namespace.get() ;
        }else {
            return null ;
        }

    }


    /**
     * 加锁处理线程安全
     *
     */

    private void fillNameSpace(){
        if(databases.isEmpty()){
            return;
        }
        Iterator<String> iterator = databases.iterator();
        while (iterator.hasNext()){

            currentdatabaseName = iterator.next();

            List<NameSpaceRange> rangeList = this.getRangeList(currentdatabaseName);

            namespaces.addAll(rangeList) ;

            iterator.remove();

            if(nameSpacehasNext()){
                break;
            }

        }

    }



    public  boolean  remove(NameSpaceRange spaceRange){
        return namespaces.remove(spaceRange);

    }
    public boolean add(NameSpaceRange spaceRange){
        //do check status
        return this.namespaces.add(spaceRange);
    }







    /**
     * 获取库表的范围列表
     *
     * @param dbName 库表名
     * @return List<Range>
     * 库表的范围列表
     */
    public List<NameSpaceRange> getRangeList(String dbName) {

        MongoIterable<String> tables = mongoClient.getDatabase(dbName).listCollectionNames();
        List<NameSpaceRange>  result = new ArrayList<>();


        for(String table :tables){
            MongoNamespace namespace = new MongoNamespace(dbName, table);
            List<NameSpaceRange> rangeList = this.getRangeList(namespace);

            result.addAll(rangeList);
        }

        return result ;

    }



    public Document getConditon(Range range){

        if(range.isMax()){
            return new Document(range.getColumnName(), new Document("$gte",range.getMinValue()).append("$lte",range.getMaxValue()));
        }else {
            return new Document(range.getColumnName(), new Document("$gte",range.getMinValue()).append("$lt",range.getMaxValue()));
        }

    }

}
