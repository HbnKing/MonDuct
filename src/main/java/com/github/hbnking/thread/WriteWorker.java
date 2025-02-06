package com.github.hbnking.thread;

import com.github.hbnking.config.AppConfig;
import com.github.hbnking.sync.DisruptorWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public class WriteWorker implements Callable<Void> {
    private static final Logger logger = LoggerFactory.getLogger(WriteWorker.class);
    private final DisruptorWriter disruptorWriter;

    public WriteWorker(AppConfig config, DisruptorWriter disruptorWriter) {
        this.disruptorWriter = disruptorWriter;
    }

    @Override
    public Void call() {
        try {
            disruptorWriter.start();
        } catch (Exception e) {
            logger.error("Error starting DisruptorWriter in WriteWorker", e);
        }
        return null;
    }
}