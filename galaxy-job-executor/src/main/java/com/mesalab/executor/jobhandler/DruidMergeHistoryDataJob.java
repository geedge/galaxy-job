package com.mesalab.executor.jobhandler;

import cn.hutool.log.Log;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.geedgenetworks.utils.StringUtil;
import com.google.common.base.Preconditions;
import com.mesalab.executor.core.config.StorgeConfig;
import com.mesalab.executor.core.utils.SpringContextUtil;
import com.mesalab.executor.pojo.DruidShouldCompactResult;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;


/**
 * 冷数据，修改查询粒度，减少存储
 */
@Component
public class DruidMergeHistoryDataJob {
    private Log logger = Log.get();

    private static StorgeConfig updateConfig =  (StorgeConfig) SpringContextUtil.getBean("storgeConfig");


    @XxlJob("druidMergeHistoryDataJobHandler")
    public ReturnT<String> druidMergeHistoryDataJobHandler (String params)  {
        final String server = updateConfig.getAnalyticServer();

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
                        if(availableSlots >= shouldCompactResult.maxNumConcurrentSubTasks){
                            availableSlots -= shouldCompactResult.maxNumConcurrentSubTasks;
                            param.getJSONObject("tuningConfig").put("maxNumConcurrentSubTasks", shouldCompactResult.maxNumConcurrentSubTasks);
                            druidCompactUtil.submitCompactTask(param, submittedTaskParams);
                        }else{
                            XxlJobLogger.log("availableSlots is too small:" + availableSlots);
                        }
                    }else if(shouldCompactResult.maxNumConcurrentSubTasks * 6 >= maxNumConcurrentSubTasks){
                        if(availableSlots >= maxNumConcurrentSubTasks){
                            availableSlots -= maxNumConcurrentSubTasks;
                            druidCompactUtil.submitCompactTask(param, submittedTaskParams);
                        }else{
                            XxlJobLogger.log("availableSlots is too small:" + availableSlots);
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
                            if(availableSlots >= shouldCompactResult.maxNumConcurrentSubTasks){
                                availableSlots -= shouldCompactResult.maxNumConcurrentSubTasks;
                                param.getJSONObject("tuningConfig").put("maxNumConcurrentSubTasks", shouldCompactResult.maxNumConcurrentSubTasks);
                                druidCompactUtil.submitCompactTask(param, submittedTaskParams);
                            }else{
                                XxlJobLogger.log("availableSlots is too small:" + availableSlots);
                            }
                        }else if(shouldCompactResult.maxNumConcurrentSubTasks * 6 >= maxNumConcurrentSubTasks){
                            if(availableSlots >= maxNumConcurrentSubTasks){
                                availableSlots -= maxNumConcurrentSubTasks;
                                druidCompactUtil.submitCompactTask(param, submittedTaskParams);
                            }else{
                                XxlJobLogger.log("availableSlots is too small:" + availableSlots);
                            }
                        }else{
                            XxlJobLogger.log("maxNumConcurrentSubTasks is too small:" + maxNumConcurrentSubTasks);
                        }
                    }
                }
            }

            XxlJobLogger.log("availableSlots:" + availableSlots);
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

        JSONArray paramArray = JSON.parseArray(params);
        for (int i = 0; i < paramArray.size(); i++) {
            JSONObject param = paramArray.getJSONObject(i);
            String paramStr = param.toString();
            Preconditions.checkArgument("compact".equals(param.getString("type")), "type must is compact:%s", paramStr);
            Preconditions.checkArgument(param.getString("dataSource") != null, "dataSource is required:%s", paramStr);
            // 冷数据必须显式配置
            Preconditions.checkArgument(StringUtils.isNotBlank(DruidCompactUtil.extractQueryGranularityFromParam(param)), "queryGranularity is required:%s", paramStr);
            String interval = param.getString("interval");

            int offsetDays = param.getIntValue("offsetDays", 30);
            param.remove("offsetDays");
            Preconditions.checkArgument(offsetDays >= 1, "offsetDays must must > 0:%s", offsetDays);
            LocalDateTime defaultEndDateTime = now.minusDays(offsetDays); // 结束时间往前推30天
            LocalDateTime defaultStartDateTime = defaultEndDateTime.plusDays(-1);

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
