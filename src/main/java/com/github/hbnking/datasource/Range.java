/*
 * Document Data Transfer - An open-source project licensed under GPL+SSPL
 *
 * Copyright (C) [2023 - present ] [Whaleal]
 *
 * This program is free software; you can redistribute it and/or modify it under the terms
 * of the GNU General Public License and Server Side Public License (SSPL) as published by
 * the Free Software Foundation; either version 2 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License and SSPL for more details.
 *
 * For more information, visit the official website: [www.whaleal.com]
 */
package com.github.hbnking.datasource;


import lombok.*;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author: lhp
 * @time: 2021/7/19 5:02 下午
 * @desc: 数据分片范围
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
@Builder
@Log4j2
public class Range implements Comparable<Range>{
    /**
     * 切分的字段名
     */
    private String columnName;
    /**
     * 最大值
     */
    private Object maxValue;
    /**
     * 最小值
     */
    private Object minValue;
    /**
     * 是否为边缘值
     * 否[min,max)
     * 是[min,max]
     */
    private boolean isMax = false;
    /**
     * 库表名
     */
    private String ns;
    /**
     * 查询范围条数
     */
    private int rangeSize;
    /**
     * 每条文档的平均大小
     */
    private long avgObjSize;

    /**
     * status  用于标记这个范围的状态
     * 初始化时 可用
     * 被领取时 则被占用
     */
    private AtomicBoolean  status =  new AtomicBoolean(false);


    @Override
    public int compareTo(Range other) {
        // 比较 columnName
        int columnNameComparison = compareNullableStrings(this.columnName, other.columnName);
        if (columnNameComparison != 0) {
            return columnNameComparison;
        }

        // 比较 maxValue
        int maxValueComparison = compareObjects(this.maxValue, other.maxValue);
        if (maxValueComparison != 0) {
            return maxValueComparison;
        }

        // 比较 minValue
        int minValueComparison = compareObjects(this.minValue, other.minValue);
        if (minValueComparison != 0) {
            return minValueComparison;
        }

        // 比较 isMax
        if (this.isMax != other.isMax) {
            return this.isMax ? 1 : -1;
        }

        // 比较 ns
        int nsComparison = compareNullableStrings(this.ns, other.ns);
        if (nsComparison != 0) {
            return nsComparison;
        }

        // 比较 rangeSize
        if (this.rangeSize < other.rangeSize) {
            return -1;
        } else if (this.rangeSize > other.rangeSize) {
            return 1;
        }

        // 比较 avgObjSize
        if (this.avgObjSize < other.avgObjSize) {
            return -1;
        } else if (this.avgObjSize > other.avgObjSize) {
            return 1;
        }


        return 0;
    }

    private int compareNullableStrings(String s1, String s2) {
        if (s1 == null && s2 == null) {
            return 0;
        }
        if (s1 == null) {
            return -1;
        }
        if (s2 == null) {
            return 1;
        }
        return s1.compareTo(s2);
    }


    private int compareObjects(Object o1, Object o2) {
        if (o1 == null && o2 == null) {
            return 0;
        }
        if (o1 == null) {
            return -1;
        }
        if (o2 == null) {
            return 1;
        }
        if (o1 instanceof Comparable && o2 instanceof Comparable) {
            return ((Comparable) o1).compareTo(o2);
        }
        return o1.toString().compareTo(o2.toString());
    }

}
