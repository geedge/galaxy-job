package com.xxl.job.executor.service.modelService;

import com.alibaba.fastjson.JSONObject;
import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.executor.common.utils.ToolUtils;
import com.zdjizhi.utils.JsonMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@Slf4j
public class BaseService {

    @Autowired
    private RestTemplate httpClientTemplate;

    /**
     * GET  请求
     * @param url
     * @param paramMap
     * @return
     */
    public JSONObject httpGet(String url, Map<String, Object> paramMap) {
        String queryUrl = ToolUtils.getUrlWithParams(url, paramMap);
        log.info("查询url为：{}", queryUrl);
        XxlJobLogger.log("查询url为：{}", queryUrl);
        ResponseEntity<JSONObject> result = httpClientTemplate.getForEntity(queryUrl, JSONObject.class);
//        JSONObject data = restTemplate.getForObject(queryUrl, JSONObject.class);
        log.debug("查询结果：{}", result);
        if (!String.valueOf(HttpStatus.SC_OK).equals(result.getStatusCode())) {
            log.error("query faild");
        }
        log.info("执行任务结果为：{}", JsonMapper.toJsonString(result));
        XxlJobLogger.log("获取执行结果数据：" + JsonMapper.toJsonString(result));
        return result.getBody();
    }

    /**
     * POST 请求
     *
     * @param url   请求路径
     * @param data  body数据
     * @param token JWT所需的Token，不需要的可去掉
     * @return
     */
    public String httpPost(String url, String data, String token) {
        HttpHeaders headers = new HttpHeaders();
        headers.add("Accept", "application/json");
        headers.add("Content-Encoding", "UTF-8");
        headers.add("Content-Type", "application/json; charset=UTF-8");
        if (token != null) {
            headers.add("Authorization", token);
        }
        HttpEntity<String> requestEntity = new HttpEntity<>(data, headers);
        String response = httpClientTemplate.postForObject(url, requestEntity, String.class);
        return response;
    }

}
