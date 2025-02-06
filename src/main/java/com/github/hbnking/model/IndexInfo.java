package com.github.hbnking.model;

import org.bson.Document;

/**
 * @author hbn.king
 * @date 2025/1/31 23:51
 * @description:
 */ // 索引信息类，用于存储索引相关的详细信息
public class IndexInfo {
    private int version;
    private Document key;
    private String name;
    private Document weights;
    private String defaultLanguage;
    private String languageOverride;
    private int textIndexVersion;
    private double expireAfterSeconds;
    private double min;
    private double max;
    private Document partialFilterExpression;
    private boolean hidden;
    private boolean sparse;
    private Document collation;

    // 构造函数，从文档中初始化索引信息
    public IndexInfo(Document indexDoc) {
        this.version = indexDoc.getInteger("v", 2);
        this.key = indexDoc.get("key", Document.class);
        this.name = indexDoc.getString("name");
        this.weights = indexDoc.get("weights", Document.class);
        this.defaultLanguage = indexDoc.getString("default_language");
        this.languageOverride = indexDoc.getString("language_override");
        this.textIndexVersion = indexDoc.getInteger("textIndexVersion", 0);
        this.expireAfterSeconds = indexDoc.getDouble("expireAfterSeconds");
        this.min = indexDoc.getDouble("min");
        this.max = indexDoc.getDouble("max");
        this.partialFilterExpression = indexDoc.get("partialFilterExpression", Document.class);
        this.hidden = indexDoc.getBoolean("hidden", false);
        this.sparse = indexDoc.getBoolean("sparse", false);
        this.collation = indexDoc.get("collation", Document.class);
    }

    // Getter 方法
    public int getVersion() {
        return version;
    }

    public Document getKey() {
        return key;
    }

    public String getName() {
        return name;
    }

    public Document getWeights() {
        return weights;
    }

    public String getDefaultLanguage() {
        return defaultLanguage;
    }

    public String getLanguageOverride() {
        return languageOverride;
    }

    public int getTextIndexVersion() {
        return textIndexVersion;
    }

    public double getExpireAfterSeconds() {
        return expireAfterSeconds;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public Document getPartialFilterExpression() {
        return partialFilterExpression;
    }

    public boolean isHidden() {
        return hidden;
    }

    public boolean isSparse() {
        return sparse;
    }

    public Document getCollation() {
        return collation;
    }

    @Override
    public String toString() {
        return "IndexInfo{" +
                "version=" + version +
                ", key=" + key +
                ", name='" + name + '\'' +
                ", weights=" + weights +
                ", defaultLanguage='" + defaultLanguage + '\'' +
                ", languageOverride='" + languageOverride + '\'' +
                ", textIndexVersion=" + textIndexVersion +
                ", expireAfterSeconds=" + expireAfterSeconds +
                ", min=" + min +
                ", max=" + max +
                ", partialFilterExpression=" + partialFilterExpression +
                ", hidden=" + hidden +
                ", sparse=" + sparse +
                ", collation=" + collation +
                '}';
    }
}
