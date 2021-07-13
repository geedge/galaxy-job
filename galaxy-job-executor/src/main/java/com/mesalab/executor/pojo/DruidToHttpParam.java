package com.mesalab.executor.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * @ Author pengfeixu
 * @ Date 2021/7/6
 * @ Description
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DruidToHttpParam {
    private String url;
    private String port;
    private String interval;
    private String segmentGranularity;
    private Map<String,Object> requestBody;// http post请求参数

}
