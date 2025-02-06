package com.github.hbnking.buffer;

/**
 * @author hbn.king
 * @date 2025/2/1 12:53
 * @description:
 */


import com.github.hbnking.model.OplogEntry;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventSink;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Disruptor 缓冲区管理类，管理多个 Disruptor 实例
 */

@Data
public class DisruptorBuffer {
    private final List<Disruptor<OplogEntryEvent>> disruptors;


    public DisruptorBuffer(int bufferSize, int disruptorCount) {

        disruptors = new ArrayList<>(disruptorCount);

        ExecutorService executor = Executors.newFixedThreadPool(disruptorCount);
        for (int i = 0; i < disruptorCount; i++) {
            Disruptor<OplogEntryEvent> disruptor = new Disruptor<>(
                    new OplogEntryEventFactory(),
                    bufferSize,
                    executor,
                    ProducerType.MULTI,
                    new BlockingWaitStrategy()
            );
            disruptors.add(disruptor);

        }
    }




    /**
     * 将 OplogEntry 放入指定分区的 Disruptor 中
     * @param partitionIndex 分区索引
     * @param entry Oplog 条目
     */
    public void put(int partitionIndex, OplogEntry entry) {
        RingBuffer<OplogEntryEvent> ringBuffer = disruptors.get(partitionIndex).getRingBuffer();
        long sequence = ringBuffer.next();
        try {
            OplogEntryEvent event = ringBuffer.get(sequence);
            event.setOplogEntry(entry);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    /**
     * 获取 Disruptor 的数量
     * @return Disruptor 的数量
     */
    public int getDisruptorCount() {
        return disruptors.size();
    }

    /**
     * 关闭所有 Disruptor 实例
     */
    public void shutdown() {
        for (Disruptor<OplogEntryEvent> disruptor : disruptors) {
            disruptor.shutdown();
        }
    }

    public RingBuffer<OplogEntryEvent> getRingBuffer(int partition) {
        return this.disruptors.get(partition).getRingBuffer();
    }

    /**
     * OplogEntry 事件类，用于 Disruptor 传递数据
     */
    public static class OplogEntryEvent {
        private OplogEntry oplogEntry;

        public OplogEntry getOplogEntry() {
            return oplogEntry;
        }

        public void setOplogEntry(OplogEntry oplogEntry) {
            this.oplogEntry = oplogEntry;
        }
    }

    /**
     * OplogEntry 事件工厂类，用于创建 OplogEntryEvent 实例
     */
    public static class OplogEntryEventFactory implements EventFactory<OplogEntryEvent> {
        @Override
        public OplogEntryEvent newInstance() {
            return new OplogEntryEvent();
        }
    }
}