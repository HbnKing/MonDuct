package com.github.hbnking.controller;

import com.github.hbnking.service.SyncService;
import com.github.hbnking.sync.SyncMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * 同步控制器，处理与数据同步相关的 HTTP 请求
 */
@RestController
@RequestMapping("/sync")
public class SyncController {

    @Autowired
    private SyncService syncService;

    /**
     * 启动同步任务
     * @param mode 同步模式，从请求参数中获取
     * @return 响应实体，包含操作结果信息和 HTTP 状态码
     */
    @PostMapping("/start")
    public ResponseEntity<String> startSync(@RequestParam("mode") String mode) {
        try {
            SyncMode syncMode = SyncMode.valueOf(mode.toUpperCase());
            syncService.startSync(syncMode);
            return new ResponseEntity<>("同步任务已启动，模式: " + mode, HttpStatus.OK);
        } catch (IllegalArgumentException e) {
            return new ResponseEntity<>("不支持的同步模式: " + mode, HttpStatus.BAD_REQUEST);
        } catch (Exception e) {
            return new ResponseEntity<>("启动同步任务时出错: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * 停止同步任务
     * @return 响应实体，包含操作结果信息和 HTTP 状态码
     */
    @PostMapping("/stop")
    public ResponseEntity<String> stopSync() {
        try {
            syncService.stopSync();
            return new ResponseEntity<>("同步任务已停止", HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<>("停止同步任务时出错: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * 获取当前同步状态
     * @return 响应实体，包含当前同步状态信息和 HTTP 状态码
     */
    @GetMapping("/status")
    public ResponseEntity<String> getSyncStatus() {
        try {
            String status = syncService.getSyncStatus();
            return new ResponseEntity<>(status, HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<>("获取同步状态时出错: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}