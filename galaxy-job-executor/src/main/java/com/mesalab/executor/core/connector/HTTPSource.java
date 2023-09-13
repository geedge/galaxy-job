package com.mesalab.executor.core.connector;

import cn.hutool.core.bean.BeanUtil;
import com.alibaba.fastjson.JSON;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.exception.BusinessException;
import com.mesalab.executor.pojo.HttpParam;
import com.xxl.job.core.handler.IJobHandler;
import org.apache.commons.lang3.ObjectUtils;

import java.util.Map;
import java.util.stream.Stream;

public class HTTPSource implements Source {
    private HttpParam httpParam;

    public HTTPSource(HttpParam httpParam) {
        this.httpParam = httpParam;
    }

    public HTTPSource(Map<String, String> sourceParams) {
        this.httpParam = BeanUtil.toBean(sourceParams, HttpParam.class);
    }

    @Override
    public Stream<String> fetchData() {
        String url = httpParam.getUrl();
        Map<String, Object> requestParams = httpParam.getRequestParams();
        if (ObjectUtils.isNotEmpty(requestParams)) {
            url = url + HttpClientUtils.getUrlWithParams(url, requestParams);
        }

        String result = "";
        if ("get".equalsIgnoreCase(httpParam.getMethod())) {
            result = HttpClientUtils.httpPost(url, JSON.toJSONString(httpParam.getRequestBody()));

        } else if ("post".equalsIgnoreCase(httpParam.getMethod())) {
            result = HttpClientUtils.httpGet(url);
        } else {
            throw new BusinessException(IJobHandler.FAIL.getCode(), "Not support Method:" + httpParam.getMethod());
        }

        if ("-1".equals(result)) {
            throw new BusinessException(IJobHandler.FAIL.getCode(), "Http client request error -1");
        }
        return Stream.of(result);
    }

    // socks代理服务器的地址与端口
/*    @Deprecated
    public static void socketProxy(Map<String, Object> requestParams) {
        if (ObjectUtils.isNotEmpty(requestParams.get("proxy"))) {
            try {
                Properties prop = System.getProperties();
                prop.put("proxySet", true);
                prop.setProperty("socksProxyHost", String.valueOf(requestParams.get("proxyHost")));
                prop.setProperty("socksProxyPort", String.valueOf(requestParams.get("proxyPort")));
            } catch (Exception e) {
                throw new BusinessException("socket proxy set error " + e.getMessage());
            }
            requestParams.remove("proxyHost");
            requestParams.remove("proxyPort");
        }
    }
    @Deprecated
    public static void removeSocketProxy() {
        try {
            Properties prop = System.getProperties();
            prop.remove("proxySet");
            prop.remove("socksProxyHost");
            prop.remove("socksProxyPort");
        } catch (Exception e) {
            throw new BusinessException("Error removing socket proxy: " + e.getMessage());
        }
    }*/
}