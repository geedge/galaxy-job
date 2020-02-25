package com.xxl.job.executor.common.config.clickhouse;

import lombok.Data;

import java.io.Serializable;
@Data
public class ClickHouseHttpSource implements Serializable {

    private String url;

    private String dbname;

    private String username;

    private String password;

    private long socketTimeout;

    private int enableHttpCompression = 1;

    private String partitionKey;



















}
