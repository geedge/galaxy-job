package com.mesalab.executor.pojo;

import java.io.Serializable;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HttpToKafkaParam implements Serializable {
    private String url;
    private String topic;
    private String method;
    private String resultKey;//指定结果集返回值
    private Map<String, Object> requestBody;// http post请求参数
}
