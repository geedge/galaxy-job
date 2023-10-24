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

/**
 * @author: zhq
 * @date: 2023/9/21
 */
public class HttpSource implements Source {
    private HttpParam httpParam;

    public HttpSource(HttpParam httpParam) {
        this.httpParam = httpParam;
    }

    public HttpSource(Map<String, String> sourceParams) {
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

}