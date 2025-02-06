package com.github.hbnking.datasource;


import com.mongodb.MongoNamespace;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Log4j2
@ToString
public class NameSpaceRange  implements Comparable<NameSpaceRange>  {
    /**
     * range
     */
    private Range range;
    /**
     * mongoNamespace
     */
    private MongoNamespace namespace;


    /**
     * 开始时间
     */
    private long startTime;
    /**
     * 结束时间
     */
    private long endTime;


    public NameSpaceRange(Range range, MongoNamespace ns) {
        this.range = range;
        this.namespace = ns;
    }



    @Override
    public int compareTo(NameSpaceRange other) {
        // 首先比较 namespace
        int namespaceComparison = this.namespace.getFullName().compareTo(other.namespace.getFullName());
        if (namespaceComparison != 0) {
            return namespaceComparison;
        }

        // 如果 startTime 和 endTime 都相同，比较 range（这里假设 Range 类实现了 toString 方法）
        return this.range.compareTo(other.range);
    }
}
