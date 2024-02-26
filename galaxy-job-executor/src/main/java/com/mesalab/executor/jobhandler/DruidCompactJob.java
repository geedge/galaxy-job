package com.mesalab.executor.jobhandler;

import cn.hutool.log.Log;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.fasterxml.jackson.databind.JavaType;
import com.geedgenetworks.utils.JsonMapper;
import com.geedgenetworks.utils.StringUtil;
import com.google.common.base.Preconditions;
import com.mesalab.executor.core.config.StorgeConfig;
import com.mesalab.executor.core.utils.SpringContextUtil;
import com.mesalab.executor.pojo.DruidShouldCompactResult;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;


/**
 * @ Author pengfeixu
 * @ Date 2021/7/6
 * @ Description druid压缩segment任务
 */
@Component
public class DruidCompactJob {
    private Log logger = Log.get();

    private static StorgeConfig compactConfig =  (StorgeConfig) SpringContextUtil.getBean("storgeConfig");

    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
    private static JavaType hashmapJsonType = jsonMapperInstance.createCollectionType(HashMap.class, String.class, Object.class);

    @XxlJob("druidCompactSegmentJobHandler")
    public ReturnT<String> druidCompactSegmentJobHandler(String params)  {
        final String server = compactConfig.getAnalyticServer();

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
                String[] times = param.getString("interval").split("/");
                final String startTime = times[0];
                final String endTime = times[1];
                String startDate = startTime;
                String endDate = endTime;

                // 查询周期内是否有数据，有数据则执行compact操作
                shouldCompactResult = druidCompactUtil.shouldCompact(dataSource, startDate, endDate, "SECOND");
                XxlJobLogger.log(String.format("%s[%s, %s]", dataSource, startDate, endDate) + " compact:" + shouldCompactResult);

                if (shouldCompactResult.shouldCompact){
                    //compact任务
                    druidCompactUtil.submitCompactTask(param, submittedTaskParams);
                }

                // 检查补录前两天compact任务
                for (int j = 1; j <= 2; j++) {
                    startDate = LocalDate.parse(startTime).minusDays(j).toString();
                    endDate = LocalDate.parse(startDate).plusDays(1).toString();

                    shouldCompactResult = druidCompactUtil.shouldCompact(dataSource, startDate, endDate, "SECOND");
                    if (shouldCompactResult.shouldCompact){
                        XxlJobLogger.log(String.format("%s[%s, %s]", dataSource, startDate, endDate) + " compact:" + shouldCompactResult);
                        param = paramTemplate.clone();
                        param.put("interval", startDate + "/" + endDate);
                        druidCompactUtil.submitCompactTask(param, submittedTaskParams);
                    }
                }
            }

            if(submittedTaskParams.size() > 0 ){
                druidCompactUtil.deleteMetaDataAsync(submittedTaskParams);
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
