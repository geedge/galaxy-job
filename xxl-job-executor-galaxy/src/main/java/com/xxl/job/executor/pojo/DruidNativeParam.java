package com.xxl.job.executor.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DruidNativeParam {
    private String queryType;
    private String dataSource;
    private String granularity;
    private String metric;
    private String dimension;
    private int threshold;
    private List<Object> aggregations;
    private List<Object> postAggregations;
    private List<Object> intervals;
}
