package com.github.hbnking.sync;

import com.github.hbnking.buffer.DisruptorBuffer;

/**
 * @author hbn.king
 * @date 2025/2/5 18:40
 * @description:
 */
public interface Iwrite {

    void context(DisruptorBuffer buffer);

    void  write();
}
