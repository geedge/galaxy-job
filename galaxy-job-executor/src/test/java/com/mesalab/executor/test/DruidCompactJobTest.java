package com.mesalab.executor.test;


import cn.hutool.log.Log;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.geedgenetworks.utils.StringUtil;
import com.google.common.base.Preconditions;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.core.utils.UrlUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.log.XxlJobLogger;
import groovy.util.logging.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @ Author pengfeixu
 * @ Date 2021/7/12
 * @ Description
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Slf4j
public class DruidCompactJobTest {
    private Log logger = Log.get();

    /**
     * 压缩测试
     *
     * 原提交请求方式
     * curl -X 'POST' -H 'Content-Type:application/json -d @*.json http://ip:8089/druid/indexer/v1/task
     */
    @Test
    public void  compactTest() throws Exception {
        String params = "[\n" +
                "    {\n" +
                "      \"type\": \"compact\",\n" +
                "      \"dataSource\": \"object_statistics_test\",\n" +
                "      \"interval\": \"2023-12-17/2023-12-18\",\n" +
                "      \"tuningConfig\" : {\n" +
                "        \"type\" : \"index_parallel\"\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"type\": \"compact\",\n" +
                "      \"dataSource\": \"top_server_ips\",\n" +
                "      \"interval\": 2,\n" +
                "      \"tuningConfig\" : {\n" +
                "        \"type\" : \"index_parallel\"\n" +
                "      }\n" +
                "    }\n" +
                "]";

        druidCompactSegmentJobHandler(params);
        System.out.println(111);
        Thread.sleep(1000 * 60 * 5);
    }

    public ReturnT<String> druidCompactSegmentJobHandler(String params)  {
        //final String server = compactConfig.getAnalyticServer();
        final String server = "192.168.44.12:8089";

        try {
            if (StringUtil.isEmpty(params) || !JSON.isValidArray(params)){
                XxlJobLogger.log("params格式为json array");
                return IJobHandler.FAIL;
            }
            JSONArray paramArray = parseParams(params);
            List<JSONObject> submittedTaskParams = new ArrayList<>();

            for (int i = 0; i < paramArray.size(); i++) {
                JSONObject param = paramArray.getJSONObject(i);
                String paramJson = param.toJSONString();
                String dataSource = param.getString("dataSource");
                String[] times = param.getString("interval").split("/");
                String startTime = times[0];
                String endTime = times[1];

                // 查询周期内是否有数据，有数据则执行compact操作
                String queryUrl = UrlUtil.getUrl(server, "/druid/v2/sql/");
                String querySql="{ \"query\" : \"select count(1) as dataCount from "+ dataSource +" where __time >= '"+ startTime+"'  and __time < '"+ endTime+"' \", \"resultFormat\" : \"objectLines\" }";
                String queryResponseValue = HttpClientUtils.httpPost(queryUrl, querySql);

                if (StringUtil.isNotBlank(queryResponseValue)){
                    if(JSON.parseObject(queryResponseValue).getLongValue("dataCount") == 0L){
                        continue;
                    }

                    //compact任务
                    XxlJobLogger.log("compact task param:" + paramJson);
                    String taskUrl = UrlUtil.getUrl(server, "/druid/indexer/v1/task");
                    String result = HttpClientUtils.httpPost(taskUrl, paramJson);
                    XxlJobLogger.log("compact task response:" + result);

                    // 提交成功的任务，等任务结束，删除废弃的数据
                    if(JSON.isValidObject(result)){
                        String taskId = JSON.parseObject(result).getString("task");
                        param.put("id", taskId);
                        submittedTaskParams.add(param);
                    }
                }
            }

            if(submittedTaskParams.size() > 0){
                deleteMetaData(submittedTaskParams);
            }
        } catch (Exception e) {
            logger.error("druidCompactSegmentJobHandler error", e);
            XxlJobLogger.log(e.getMessage());
            return ReturnT.FAIL;
        }

        return ReturnT.SUCCESS;
    }

    public JSONArray parseParams(String params) {
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC")); // 默认结束时间是UTC时间
        LocalDateTime defaultEndDateTime = now.getHour() >= 1? now: now.plusDays(-1); // 如果hour在1小时内, 结束时间往前推一天
        LocalDateTime defaultStartDateTime = defaultEndDateTime.plusDays(-1);

        JSONArray paramArray = JSON.parseArray(params);
        for (int i = 0; i < paramArray.size(); i++) {
            JSONObject param = paramArray.getJSONObject(i);
            String paramStr = param.toString();
            Preconditions.checkArgument("compact".equals(param.getString("type")), "type must is compact:%s", paramStr);
            Preconditions.checkArgument(param.getString("dataSource") != null, "dataSource is required:%s", paramStr);
            String interval = param.getString("interval");
            /**
             * interval:
             *   null:默认周期
             *   yyyy-MM-dd/yyyy-MM-dd:自定义周期
             *   intVal:[默认结束周期 - intVal天, 默认结束周期)
             */
            if(interval == null){
                param.put("interval", dateFormatter.format(defaultStartDateTime) + "/" + dateFormatter.format(defaultEndDateTime));
            } else if (!interval.contains("/")) {
                LocalDateTime startDateTime = defaultEndDateTime.minusDays(Integer.parseInt(interval));
                param.put("interval", dateFormatter.format(startDateTime) + "/" + dateFormatter.format(defaultEndDateTime));
            }
        }

        return paramArray;
    }

    /**
     * 压缩任务通过http请求提交后，获取任务状态码，根据任务状态删除历史元数据
     */
    public void deleteMetaData(List<JSONObject> submittedTaskParams){
        //final String server = compactConfig.getAnalyticServer();
        final String server = "192.168.44.12:8089";
        //查询任务状态限时2小时
        long startTime = System.currentTimeMillis();
        final long endTime = startTime + 1000 * 60 * 120;
        new Thread(() -> {
            while (submittedTaskParams.size() > 0 && System.currentTimeMillis() < endTime){
                try {
                    Iterator<JSONObject> iterator = submittedTaskParams.iterator();
                    while (iterator.hasNext()){
                        JSONObject param = iterator.next();
                        String taskId = param.getString("id");
                        String dataSource = param.getString("dataSource");
                        String interval = param.getString("interval");
                        String statusUrl = UrlUtil.getUrl(server, "/druid/indexer/v1/task") + String.format("/%s/status", taskId);
                        String response = HttpClientUtils.httpGet(statusUrl);
                        JSONObject result = JSON.parseObject(response);
                        String statusCode = result.getJSONObject("status").getString("statusCode");
                        logger.info(String.format("task(%s) statu:%s", taskId, response));
                        if("SUCCESS".equals(statusCode)){
                            iterator.remove();
                            String url = UrlUtil.getUrl(server, "/druid/indexer/v1/task");
                            String deleteJson = "{\"type\": \"kill\"," +
                                    "\"dataSource\"" + ":" + "\"" + dataSource + "\"," +
                                    "\"interval\"" + ":" + "\"" + interval + "\"}";
                            logger.info("deleteMetaData:" + deleteJson);
                            HttpClientUtils.httpPost(url, deleteJson);
                        }else if("FAILED".equals(statusCode)){
                            iterator.remove();
                            logger.warn(String.format("task(%s) is failed:%s", taskId, response));
                        }
                    }

                    if(submittedTaskParams.size() > 0){
                        Thread.sleep(1000 * 60);
                    }
                } catch (Exception e) {
                    logger.error("deleteMetaData", e);
                    try {
                        Thread.sleep(1000 * 60);
                    }catch (Exception ex) {
                    }
                }
            }
            logger.info("DruidCompactDeleteMetaDataThread finished, wait delete size:" + submittedTaskParams.size());
        }, "DruidCompactDeleteMetaData").start();
    }
}
