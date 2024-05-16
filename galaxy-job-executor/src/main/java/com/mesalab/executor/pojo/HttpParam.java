package com.mesalab.executor.pojo;

import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.xxl.job.core.log.XxlJobLogger;
import io.netty.handler.codec.http.HttpMethod;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
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
    private Map<String, Object> requestParams;//请求表单

    public static List<HttpParam> parserHttpParams(String params) {
        if (StrUtil.isBlank(params)) {
            XxlJobLogger.log("params is empty!");
            return null;
        }
        List<HttpParam> httpParams = JSONUtil.toList(params, HttpParam.class);
        for (HttpParam httpParam : httpParams) {
            if (ObjectUtil.isNull(httpParam)) {
                XxlJobLogger.log("params is not format json ! ");
                return null;
            }
            if (StrUtil.isBlank(httpParam.getUrl())) {
                XxlJobLogger.log("url is empty ! ");
                return null;
            }
            if (StrUtil.isEmpty(httpParam.getStore())) {
                XxlJobLogger.log("store is empty ! ");
                return null;
            }
            if (StrUtil.isBlank(httpParam.getMethod())) {
                XxlJobLogger.log("this request method is default get");
                httpParam.setMethod(HttpMethod.GET.name());
            }
        }
        return httpParams;
    }

    public static HttpParam parserHttpParam(String params) {
        if (StrUtil.isBlank(params)) {
            XxlJobLogger.log("params is empty!");
            return null;
        }
        HttpParam httpParam = JSONUtil.toBean(params, HttpParam.class,true);
        if (ObjectUtil.isNull(httpParam)) {
            XxlJobLogger.log("params is not format json ! ");
            return null;
        }
        if (StrUtil.isBlank(httpParam.getUrl())) {
            XxlJobLogger.log("url is empty ! ");
            return null;
        }
        if (StrUtil.isEmpty(httpParam.getStore())) {
            XxlJobLogger.log("store is empty ! ");
            return null;
        }
        if (StrUtil.isBlank(httpParam.getMethod())) {
            XxlJobLogger.log("this request method is default get");
            httpParam.setMethod(HttpMethod.GET.name());
        }
        return httpParam;
    }
}
