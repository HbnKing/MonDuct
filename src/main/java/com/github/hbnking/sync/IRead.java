package com.github.hbnking.sync;

import com.github.hbnking.buffer.DisruptorBuffer;

/**
 * @author hbn.king
 * @date 2025/2/5 18:38
 * @description:
 */
public interface IRead {


    void context(DisruptorBuffer  buffer);
    void  read() ;


}
