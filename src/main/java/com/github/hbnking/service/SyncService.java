package com.github.hbnking.service;

import com.github.hbnking.sync.SyncMode;
import org.springframework.stereotype.Service;

/**
 * @author hbn.king
 * @date 2025/2/1 14:53
 * @description:
 */
@Service
public class SyncService {
    public long getProcessedEventsCount() {

        return 0l ;
    }

    public void startSync(SyncMode syncMode) {
    }

    public void stopSync() {
    }

    public String getSyncStatus() {
        return "";
    }
}
