package com.xxl.job.executor.pojo;

import java.util.Map;

import com.google.common.collect.Maps;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DruidQueryParam {
    public String query;
    public Map<String, String> context = Maps.newHashMap();
    public String resultFormat;
}
