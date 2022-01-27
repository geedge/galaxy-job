package com.mesalab.executor.pojo;

import lombok.*;

import java.io.Serializable;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HttpParam implements Serializable {
    private String url;
    private String store;
    private String method;
    private String resultKey;//指定结果集返回值
    private Map<String, Object> requestBody;// http post请求参数
}
