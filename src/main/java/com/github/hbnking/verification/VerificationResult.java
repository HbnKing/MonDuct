package com.github.hbnking.verification;

import java.util.HashMap;
import java.util.Map;

/**
 * 验证结果类，用于存储数据验证的结果信息
 */
public class VerificationResult {
    private boolean isVerified;
    private String message;
    private Map<String, Object> details;

    public VerificationResult() {
        this.details = new HashMap<>();
    }

    /**
     * 获取验证是否通过的标志
     * @return 验证通过返回 true，否则返回 false
     */
    public boolean isVerified() {
        return isVerified;
    }

    /**
     * 设置验证是否通过的标志
     * @param verified 验证通过为 true，否则为 false
     */
    public void setVerified(boolean verified) {
        isVerified = verified;
    }

    /**
     * 获取验证结果的详细信息
     * @return 验证结果的详细信息
     */
    public String getMessage() {
        return message;
    }

    /**
     * 设置验证结果的详细信息
     * @param message 验证结果的详细信息
     */
    public void setMessage(String message) {
        this.message = message;
    }

    /**
     * 获取验证结果的详细数据
     * @return 存储详细数据的 Map
     */
    public Map<String, Object> getDetails() {
        return details;
    }

    /**
     * 添加验证结果的详细数据
     * @param key 数据的键
     * @param value 数据的值
     */
    public void addDetail(String key, Object value) {
        this.details.put(key, value);
    }

    @Override
    public String toString() {
        return "VerificationResult{" +
                "isVerified=" + isVerified +
                ", message='" + message + '\'' +
                ", details=" + details +
                '}';
    }
}