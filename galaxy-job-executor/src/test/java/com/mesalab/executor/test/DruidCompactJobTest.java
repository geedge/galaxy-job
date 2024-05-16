package com.mesalab.executor.test;


import cn.hutool.log.Log;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.geedgenetworks.utils.StringUtil;
import com.google.common.base.Preconditions;
import com.mesalab.executor.jobhandler.DruidCompactUtil;
import com.mesalab.executor.pojo.DruidShouldCompactResult;
import com.mesalab.executor.pojo.DruidWaitedSubmitTaskParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.log.XxlJobLogger;
import groovy.util.logging.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
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
        /*String params = "[\n" +
                "    {\n" +
                "      \"type\": \"compact\",\n" +
                "      \"dataSource\": \"object_statistics_test\",\n" +
                "      \"tuningConfig\" : {\n" +
                "        \"type\" : \"index_parallel\"\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"type\": \"compact\",\n" +
                "      \"dataSource\": \"object_statistics\",\n" +
                "      \"tuningConfig\" : {\n" +
                "        \"type\" : \"index_parallel\"\n" +
                "      }\n" +
                "    }\n" +
                "]";*/
        String params = "[\n" +
                "    {\n" +
                "      \"type\": \"compact\",\n" +
                "      \"dataSource\": \"object_statistics_test\",\n" +
                "      \"interval\": \"2024-05-16/2024-05-17\",\n" +
                "      \"tuningConfig\" : {\n" +
                "        \"type\" : \"index_parallel\",\n" +
                "        \"maxNumConcurrentSubTasks\" : 2\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"type\": \"compact\",\n" +
                "      \"dataSource\": \"object_statistics\",\n" +
                "      \"interval\": \"2024-05-16/2024-05-17\",\n" +
                "      \"tuningConfig\" : {\n" +
                "        \"type\" : \"index_parallel\",\n" +
                "        \"maxNumConcurrentSubTasks\" : 2\n" +
                "      }\n" +
                "    }\n" +
                "]";
        System.out.println(params);

        druidCompactSegmentJobHandler(params);
        System.out.println(111);
        Thread.sleep(1000 * 60 * 10);
    }

    public ReturnT<String> druidCompactSegmentJobHandler(String params)  {
        // final String server = compactConfig.getAnalyticServer();
        final String server = "192.168.41.29:8089";

        try {
            DruidCompactUtil druidCompactUtil = DruidCompactUtil.getInstance(server);

            if (StringUtil.isEmpty(params) || !JSON.isValidArray(params)){
                XxlJobLogger.log("params格式为json array");
                return IJobHandler.FAIL;
            }

            int availableSlots = druidCompactUtil.queryAvailableSlots();
            XxlJobLogger.log("availableSlots:" + availableSlots);
            if(availableSlots <= 0){
                return ReturnT.SUCCESS;
            }

            JSONArray paramArray = parseParams(params);
            List<JSONObject> submittedTaskParams = new ArrayList<>();
            List<DruidWaitedSubmitTaskParam> waitedSubmitTaskParams = new ArrayList<>();
            JSONObject paramTemplate;
            JSONObject param;
            DruidShouldCompactResult shouldCompactResult;

            for (int i = 0; i < paramArray.size(); i++) {
                paramTemplate = paramArray.getJSONObject(i);
                param = paramTemplate.clone();
                String dataSource = param.getString("dataSource");
                int maxNumConcurrentSubTasks = param.getJSONObject("tuningConfig").getIntValue("maxNumConcurrentSubTasks", 1);
                final String queryGranularity = DruidCompactUtil.extractQueryGranularityFromParam(param);
                String[] times = param.getString("interval").split("/");
                final String startTime = times[0];
                final String endTime = times[1];
                String startDate = startTime;
                String endDate = endTime;

                // 查询周期内是否有数据，有数据则执行compact操作
                shouldCompactResult = druidCompactUtil.shouldCompact(dataSource, startDate, endDate, queryGranularity);
                XxlJobLogger.log(String.format("%s[%s, %s]", dataSource, startDate, endDate) + " compact:" + shouldCompactResult);

                if (shouldCompactResult.shouldCompact){
                    //compact任务
                    if(shouldCompactResult.maxNumConcurrentSubTasks < maxNumConcurrentSubTasks){
                        param.getJSONObject("tuningConfig").put("maxNumConcurrentSubTasks", shouldCompactResult.maxNumConcurrentSubTasks);
                        if(availableSlots >= shouldCompactResult.maxNumConcurrentSubTasks){
                            availableSlots -= shouldCompactResult.maxNumConcurrentSubTasks;
                            druidCompactUtil.submitCompactTask(param, submittedTaskParams);
                        }else{
                            XxlJobLogger.log("availableSlots is too small:" + availableSlots);
                            waitedSubmitTaskParams.add(new DruidWaitedSubmitTaskParam(0, shouldCompactResult.maxNumConcurrentSubTasks, param));
                        }
                    }else if(shouldCompactResult.maxNumConcurrentSubTasks * 6 >= maxNumConcurrentSubTasks){
                        if(availableSlots >= maxNumConcurrentSubTasks){
                            availableSlots -= maxNumConcurrentSubTasks;
                            druidCompactUtil.submitCompactTask(param, submittedTaskParams);
                        }else{
                            XxlJobLogger.log("availableSlots is too small:" + availableSlots);
                            waitedSubmitTaskParams.add(new DruidWaitedSubmitTaskParam(0, maxNumConcurrentSubTasks, param));
                        }
                    }else{
                        XxlJobLogger.log("maxNumConcurrentSubTasks is too small:" + maxNumConcurrentSubTasks);
                    }
                }
            }

            XxlJobLogger.log(StringUtils.repeat('#', 100));
            XxlJobLogger.log("availableSlots:" + availableSlots);

            for (int i = 0; i < paramArray.size(); i++) {
                paramTemplate = paramArray.getJSONObject(i);
                param = paramTemplate.clone();
                String dataSource = param.getString("dataSource");
                int maxNumConcurrentSubTasks = param.getJSONObject("tuningConfig").getIntValue("maxNumConcurrentSubTasks", 1);
                final String queryGranularity = DruidCompactUtil.extractQueryGranularityFromParam(param);
                String[] times = param.getString("interval").split("/");
                final String startTime = times[0];
                final String endTime = times[1];
                String startDate = startTime;
                String endDate = endTime;

                // 检查补录前两天compact任务
                for (int j = 1; j <= 2; j++) {
                    startDate = LocalDate.parse(startTime).minusDays(j).toString();
                    endDate = LocalDate.parse(startDate).plusDays(1).toString();

                    shouldCompactResult = druidCompactUtil.shouldCompact(dataSource, startDate, endDate, queryGranularity);
                    if (shouldCompactResult.shouldCompact){
                        XxlJobLogger.log(String.format("%s[%s, %s]", dataSource, startDate, endDate) + " compact:" + shouldCompactResult);
                        param = paramTemplate.clone();
                        param.put("interval", startDate + "/" + endDate);

                        if(shouldCompactResult.maxNumConcurrentSubTasks < maxNumConcurrentSubTasks){
                            param.getJSONObject("tuningConfig").put("maxNumConcurrentSubTasks", shouldCompactResult.maxNumConcurrentSubTasks);
                            if(availableSlots >= shouldCompactResult.maxNumConcurrentSubTasks){
                                availableSlots -= shouldCompactResult.maxNumConcurrentSubTasks;
                                druidCompactUtil.submitCompactTask(param, submittedTaskParams);
                            }else{
                                XxlJobLogger.log("availableSlots is too small:" + availableSlots);
                                waitedSubmitTaskParams.add(new DruidWaitedSubmitTaskParam(j, shouldCompactResult.maxNumConcurrentSubTasks, param));
                            }
                        }else if(shouldCompactResult.maxNumConcurrentSubTasks * 6 >= maxNumConcurrentSubTasks){
                            if(availableSlots >= maxNumConcurrentSubTasks){
                                availableSlots -= maxNumConcurrentSubTasks;
                                druidCompactUtil.submitCompactTask(param, submittedTaskParams);
                            }else{
                                XxlJobLogger.log("availableSlots is too small:" + availableSlots);
                                waitedSubmitTaskParams.add(new DruidWaitedSubmitTaskParam(j, maxNumConcurrentSubTasks, param));
                            }
                        }else{
                            XxlJobLogger.log("maxNumConcurrentSubTasks is too small:" + maxNumConcurrentSubTasks);
                        }
                    }
                }
            }

            XxlJobLogger.log("availableSlots:" + availableSlots);
            if(submittedTaskParams.size() > 0 ){
                if(waitedSubmitTaskParams.size() > 0){
                    Collections.sort(waitedSubmitTaskParams);
                }
                druidCompactUtil.deleteMetaDataAsync(submittedTaskParams, waitedSubmitTaskParams);
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
}
