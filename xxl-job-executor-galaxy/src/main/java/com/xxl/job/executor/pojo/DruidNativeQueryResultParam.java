package com.xxl.job.executor.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DruidNativeQueryResultParam {
    private String protocol_type;
    private int session_num;
    private long order_num;
    private int ip_num;
    private String domain;
    private String stat_time;
}
