package com.xxl.job.executor.common.config.clickhouse;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "clickhouse")
public class ClickHouseProperties {
    private String url;

    private String driverClassName;

    private String dbname;

    private String username;

    private String password;

    private String publicKey;

}
