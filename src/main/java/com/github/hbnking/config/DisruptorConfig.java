package com.github.hbnking.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author hbn.king
 * @date 2025/2/5 16:44
 * @description:
 */


@Configuration
@ConfigurationProperties(prefix = "mongodb.sync")
@Data
public class DisruptorConfig {
}
