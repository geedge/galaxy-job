package com.mesalab.executor.test;

import cn.hutool.log.Log;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.geedgenetworks.utils.StringUtil;
import com.google.common.base.Preconditions;
import com.mesalab.executor.jobhandler.DruidCompactUtil;
import com.mesalab.executor.pojo.DruidShouldCompactResult;
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
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class DruidMergeHistoryDataJobTest {
    private Log logger = Log.get();

    @Test
    public void  compactTest() throws Exception {
      /*String params = "[\n" +
              "    {\n" +
              "      \"type\": \"compact\",\n" +
              "      \"dataSource\": \"object_statistics_test\",\n" +
              "      \"tuningConfig\" : {\n" +
              "        \"type\" : \"index_parallel\"\n" +
              "      },\n" +
              "      \"granularitySpec\" : {\n" +
              "        \"segmentGranularity\": \"DAY\",\n" +
              "        \"queryGranularity\": {\"type\": \"period\", \"period\": \"MINUTE\"}\n" +
              "      },\n" +
              "    },\n" +
              "    {\n" +
              "      \"type\": \"compact\",\n" +
              "      \"dataSource\": \"object_statistics\",\n" +
              "      \"tuningConfig\" : {\n" +
              "        \"type\" : \"index_parallel\"\n" +
              "      },\n" +
              "      \"granularitySpec\" : {\n" +
              "        \"segmentGranularity\": \"DAY\",\n" +
              "        \"queryGranularity\": {\"type\": \"period\", \"period\": \"MINUTE\"}\n" +
              "      },\n" +
              "    }\n" +
              "]";*/
        String params = "[\n" +
                "    {\n" +
                "      \"type\": \"compact\",\n" +
                "      \"dataSource\": \"object_statistics_test\",\n" +
                "      \"interval\": \"2024-02-22/2024-02-23\",\n" +
                "      \"tuningConfig\" : {\n" +
                "        \"type\" : \"index_parallel\"\n" +
                "      },\n" +
                "      \"granularitySpec\" : {\n" +
                "        \"segmentGranularity\": \"DAY\",\n" +
                "        \"queryGranularity\": \"FIVE_MINUTE\"\n" +
                "      },\n" +
                "    },\n" +
                "    {\n" +
                "      \"type\": \"compact\",\n" +
                "      \"dataSource\": \"object_statistics\",\n" +
                "      \"interval\": \"2024-02-22/2024-02-23\",\n" +
                "      \"tuningConfig\" : {\n" +
                "        \"type\" : \"index_parallel\"\n" +
                "      },\n" +
                "      \"granularitySpec\" : {\n" +
                "        \"segmentGranularity\": \"DAY\",\n" +
                "        \"queryGranularity\": \"FIVE_MINUTE\"\n" +
                "      },\n" +
                "    }\n" +
                "]";
        System.out.println(params);

        druidMergeHistoryDataJobHandler(params);
        System.out.println(111);
        Thread.sleep(1000 * 60 * 10);
    }

    public ReturnT<String> druidMergeHistoryDataJobHandler (String params)  {
        //final String server = updateConfig.getAnalyticServer();
        final String server = "192.168.41.29:8089";

        try {
            DruidCompactUtil druidCompactUtil = DruidCompactUtil.getInstance(server);

            if (StringUtil.isEmpty(params) || !JSON.isValidArray(params)){
                XxlJobLogger.log("params格式为json array");
                return IJobHandler.FAIL;
            }

            JSONArray paramArray = parseParams(params);
            List<JSONObject> submittedTaskParams = new ArrayList<>();
            JSONObject paramTemplate;
            JSONObject param;
            DruidShouldCompactResult shouldCompactResult;

            for (int i = 0; i < paramArray.size(); i++) {
                paramTemplate = paramArray.getJSONObject(i);
                param = paramTemplate.clone();
                String dataSource = param.getString("dataSource");
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
                    druidCompactUtil.submitCompactTask(param, submittedTaskParams);
                }

                // 检查补录前两天compact任务
                for (int j = 1; j <= 2; j++) {
                    startDate = LocalDate.parse(startTime).minusDays(j).toString();
                    endDate = LocalDate.parse(startDate).plusDays(1).toString();

                    shouldCompactResult = druidCompactUtil.shouldCompact(dataSource, startDate, endDate, queryGranularity);
                    if (shouldCompactResult.shouldCompact){
                        XxlJobLogger.log(String.format("%s[%s, %s]", dataSource, startDate, endDate) + " compact:" + shouldCompactResult);
                        param = paramTemplate.clone();
                        param.put("interval", startDate + "/" + endDate);
                        druidCompactUtil.submitCompactTask(param, submittedTaskParams);
                    }
                }
            }

            if(submittedTaskParams.size() > 0){
                druidCompactUtil.deleteMetaDataAsync(submittedTaskParams);
            }
        } catch (Exception e) {
            logger.error("druidMergeHistoryDataJobHandler error", e);
            XxlJobLogger.log(e.getMessage());
            return ReturnT.FAIL;
        }

        return ReturnT.SUCCESS;
    }

    public JSONArray parseParams(String params) {
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC")); // 默认结束时间是UTC时间
        LocalDateTime defaultEndDateTime = now.plusDays(-30); // 结束时间往前推30天
        LocalDateTime defaultStartDateTime = defaultEndDateTime.plusDays(-1);

        JSONArray paramArray = JSON.parseArray(params);
        for (int i = 0; i < paramArray.size(); i++) {
            JSONObject param = paramArray.getJSONObject(i);
            String paramStr = param.toString();
            Preconditions.checkArgument("compact".equals(param.getString("type")), "type must is compact:%s", paramStr);
            Preconditions.checkArgument(param.getString("dataSource") != null, "dataSource is required:%s", paramStr);
            // 冷数据必须显式配置
            Preconditions.checkArgument(StringUtils.isNotBlank(DruidCompactUtil.extractQueryGranularityFromParam(param)), "queryGranularity is required:%s", paramStr);
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
                int days = Integer.parseInt(interval);
                Preconditions.checkArgument(days > 5, "days must > 5:%s", Integer.toString(days));
                LocalDateTime startDateTime = defaultEndDateTime.minusDays(days);
                param.put("interval", dateFormatter.format(startDateTime) + "/" + dateFormatter.format(defaultEndDateTime));
            }
        }

        return paramArray;
    }
}
