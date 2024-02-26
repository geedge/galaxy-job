package com.mesalab.executor.jobhandler;

import cn.hutool.log.Log;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.core.utils.UrlUtil;
import com.mesalab.executor.druid.granularity.*;
import com.mesalab.executor.pojo.DruidShouldCompactResult;
import com.xxl.job.core.log.XxlJobLogger;
import org.apache.commons.lang3.StringUtils;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DruidCompactUtil {
    private static Log logger = Log.get();
    final private static ObjectMapper mapper;
    private static DruidCompactUtil instance;
    final private String server;

    static {
        mapper = new ObjectMapper();
        GranularityModule granularityModule = new GranularityModule();
        JodaStuff.register(granularityModule);
        mapper.registerModule(granularityModule);
    }

    private DruidCompactUtil(String server) {
        this.server = server;
    }

    public static synchronized DruidCompactUtil getInstance(String server){
        if(instance == null){
            instance = new DruidCompactUtil(server);
        }

        if(!instance.server.equals(server)){
            throw new RuntimeException(String.format("server is different:[%s, %s]", instance.server, server));
        }

        return instance;
    }

    public  DruidShouldCompactResult shouldCompact(String dataSource, String startTime, String endTime, String queryGranularity){
        String queryUrl = UrlUtil.getUrl(server, "/druid/v2/sql/");
        String responseText = querySegmentsMetas(queryUrl, "object", dataSource, startTime, endTime);
        JSONArray results = JSON.parseArray(responseText);
        // 没有需要compact的文件
        int fileCount = results.size();
        if(fileCount == 0){
            return DruidShouldCompactResult.notShould("has not segments");
        }

        boolean firstCompact = false;
        boolean queryGranularityChange = false;
        String queryGranularityChangeConf = "";
        long totalRowCount = 0;
        long totalByteSize = 0;

        for (int i = 0; i < fileCount; i++) {
            JSONObject rst = results.getJSONObject(i);
            String lastCompactionStateDesc = rst.getString("last_compaction_state");
            // 之前没有compact的记录, 首次compact
            if(StringUtils.isBlank(lastCompactionStateDesc)){
                firstCompact = true;
            }else{
                // 和上次compact配置不同, 重新compact
                JSONObject lastCompactionState = JSON.parseObject(lastCompactionStateDesc);
                String segmentQueryGranularity = extractQueryGranularityFromParam(lastCompactionState);
                if(queryGranularityRuleChange(segmentQueryGranularity, queryGranularity)){
                    queryGranularityChange = true;
                    queryGranularityChangeConf = segmentQueryGranularity;
                }
            }

            totalRowCount += rst.getLongValue("num_rows");
            totalByteSize += rst.getLongValue("size");
        }

        // 首次compact
        if(firstCompact){
            return DruidShouldCompactResult.should("never merge before", totalRowCount, totalByteSize, fileCount);
        }

        // compact配置变化
        if(queryGranularityChange){
            return DruidShouldCompactResult.should(String.format("from last compaction granularitySpec is different:[%s, %s]", queryGranularity, queryGranularityChangeConf),
                    totalRowCount, totalByteSize, fileCount);
        }

        // 有小文件
        if(fileCount > 1 && totalRowCount / (fileCount - 1) < 1000000000){
            return DruidShouldCompactResult.should("has small segments file", totalRowCount, totalByteSize, fileCount);
        }else{
            return DruidShouldCompactResult.notShould("already merged", totalRowCount, totalByteSize, fileCount);
        }
    }

    public void submitCompactTask(JSONObject param, List<JSONObject> submittedTaskParams){
        String paramJson = param.toJSONString();
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

    public void deleteMetaDataAsync(List<JSONObject> submittedTaskParams){
        //默认查询任务状态限时6小时
        deleteMetaDataAsync(submittedTaskParams, Duration.ofHours(6));
    }

    /**
     * 压缩任务通过http请求提交后，获取任务状态码，根据任务状态删除历史元数据
     */
    public void deleteMetaDataAsync(List<JSONObject> submittedTaskParams, Duration maxWaitDuration){
        final long endTime = System.currentTimeMillis() + maxWaitDuration.toMillis();
        XxlJobLogger.log("wait delete submitted tasks:" + String.join("\n", submittedTaskParams.stream().map(x -> x.toString()).collect(Collectors.toList())));
        if(submittedTaskParams.isEmpty()){
            return;
        }

        new Thread(() -> {
            logger.info("DruidCompactDeleteMetaDataThread start, wait delete size:" + submittedTaskParams.size());
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

    public static String extractQueryGranularityFromParam(JSONObject param){
        JSONObject granularitySpec = param.getJSONObject("granularitySpec");
        if(granularitySpec != null){
            return granularitySpec.getString("queryGranularity");
        }else{
            return null;
        }
    }

    private String querySegmentsMetas(String url, String resultFormat, String dataSource, String startTime, String endTime){
        String sql = String.format("select\n" +
                        "    datasource, \"start\", \"end\", version, num_rows, size, last_compaction_state\n" +
                        "from sys.segments\n" +
                        "where datasource = '%s' and is_active = 1 and is_published = 1 and is_overshadowed = 0 and \"start\" >= '%s' and \"start\" < '%s'",
                dataSource, startTime, endTime);
        Map<String, Object> params = new HashMap<>();
        params.put("query", sql);
        params.put("resultFormat", resultFormat);
        return HttpClientUtils.httpPost(url, JSON.toJSONString(params));
    }

    private static boolean queryGranularityRuleChange(String segmentQueryGranularity, String queryGranularity){
        if(StringUtils.isBlank(segmentQueryGranularity)){
            return true;
        }

        if(StringUtils.isBlank(queryGranularity)){
            return false;
        }

        // 都不为空
        try {
            Granularity segmentGranularity = parseGranularity(segmentQueryGranularity);
            Granularity granularity = parseGranularity(queryGranularity);
            return Granularity.IS_FINER_THAN.compare(granularity, segmentGranularity) > 0;
        } catch (JsonProcessingException e) {
            logger.error("parseGranularity error", e);
            return false;
        }
    }

    private static Granularity parseGranularity(String str) throws JsonProcessingException {
        Granularity granularity;

        if(str.trim().startsWith("{")){
            granularity = mapper.readValue(str, PeriodGranularity.class);
        }else{
            granularity = Granularity.fromString(str);
        }

        return granularity;
    }
}
