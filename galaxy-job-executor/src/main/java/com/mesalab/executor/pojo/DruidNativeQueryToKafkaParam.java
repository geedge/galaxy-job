package com.mesalab.executor.pojo;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DruidNativeQueryToKafkaParam {
    private String url;
    private String method;
    private Map<String,Object> requestBody;// http post请求参数
    private String topic;
}
