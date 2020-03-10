package com.xxl.job.executor.pojo;

import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RequestParam {
    public String url;
    public String method;
    public String resultKey;
    public Map<String, Object> requestBody;
}
