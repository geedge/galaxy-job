package com.mesalab.executor.jobhandler;

import cn.hutool.log.Log;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONPath;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.core.utils.UrlUtil;
import com.mesalab.executor.druid.granularity.*;
import com.mesalab.executor.pojo.DruidShouldCompactResult;
import com.mesalab.executor.pojo.DruidWaitedSubmitTaskParam;
import com.xxl.job.core.log.XxlJobLogger;
import org.apache.commons.lang3.StringUtils;

import java.time.Duration;
import java.util.*;
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

    public DruidShouldCompactResult shouldCompact(String dataSource, String startTime, String endTime, String queryGranularity){
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

        long rowSize = totalByteSize / totalRowCount;
        int maxNumConcurrentSubTasks = (int) Math.round(totalRowCount/((rowSize < 40 ? 50000D:rowSize < 60 ? 40000D: 30000D) * 60 * 60));
        if(maxNumConcurrentSubTasks < 1){
            maxNumConcurrentSubTasks = 1;
        }

        // 首次compact
        if(firstCompact){
            if(queryGranularity != null){
                return DruidShouldCompactResult.should("never merge before", totalRowCount, totalByteSize, fileCount, maxNumConcurrentSubTasks);
            }else if(hasSmallSegmentsFile(totalRowCount, fileCount)){
                return DruidShouldCompactResult.should("never merge before and has small segments file", totalRowCount, totalByteSize, fileCount, maxNumConcurrentSubTasks);
            }else{
                return DruidShouldCompactResult.notShould("queryGranularity is null and has not small segments file", totalRowCount, totalByteSize, fileCount, maxNumConcurrentSubTasks);
            }
        }

        // compact配置变化
        if(queryGranularityChange){
            return DruidShouldCompactResult.should(String.format("from last compaction granularitySpec is different:[%s, %s]", queryGranularity, queryGranularityChangeConf),
                    totalRowCount, totalByteSize, fileCount, maxNumConcurrentSubTasks);
        }

        // 有小文件
        if(hasSmallSegmentsFile(totalRowCount, fileCount)){
            return DruidShouldCompactResult.should("has small segments file", totalRowCount, totalByteSize, fileCount, maxNumConcurrentSubTasks);
        }else{
            return DruidShouldCompactResult.notShould("already merged", totalRowCount, totalByteSize, fileCount, maxNumConcurrentSubTasks);
        }
    }

    private boolean hasSmallSegmentsFile(long totalRowCount, int fileCount){
        if(fileCount <= 1){
            return false;
        }

        if(fileCount <= 400){
            if(totalRowCount / (fileCount - 1) < 2500000){
                return true;
            }
        }else{
            if(totalRowCount / (fileCount - 1) < 2000000){
                return true;
            }
        }

        return false;
    }

    public void fitStatisticsRuleOldData(JSONObject param){
        try {
            doFitStatisticsRuleOldData(param);
        } catch (Exception e) {
            XxlJobLogger.log(e);
        }
    }

    private void doFitStatisticsRuleOldData(JSONObject param){
        if(needFetchCompactMeta(param)){
            return; // 不配置参数, 任务会自动获取参数
        }

        String dataSource = param.getString("dataSource");
        String interval = param.getString("interval");
        JSONArray dimensionArray = param.getJSONObject("dimensionsSpec").getJSONArray("dimensions");
        JSONArray metricArray = param.getJSONArray("metricsSpec");
        Set<String> dimensions = new LinkedHashSet<>(dimensionArray.size() * 2);
        Set<String> metrics = new LinkedHashSet<>(metricArray.size() * 2);
        Object item;
        for (int i = 0; i < dimensionArray.size(); i++) {
            item = dimensionArray.get(i);
            if(item instanceof String){
                dimensions.add((String) item);
            }else{
                dimensions.add(((JSONObject)item).getString("name"));
            }
        }
        for (int i = 0; i < metricArray.size(); i++) {
            metrics.add(metricArray.getJSONObject(i).getString("name"));
        }

        // 摄入任务是24-08后版本
        Set<String> newVersionMetricSet = ImmutableSet.of("unique_count_1", "unique_count_2", "unique_count_3", "unique_count_4");
        boolean isNewVersion = metrics.stream().filter(x -> newVersionMetricSet.contains(x)).count() >= newVersionMetricSet.size()/2;
        if(!isNewVersion){
            return;
        }

        String segmentsUrl = UrlUtil.getUrl(server, String.format("/druid/coordinator/v1/datasources/%s/intervals/%s?full", dataSource, interval.replace('/', '_')));
        String response = HttpClientUtils.httpGet(segmentsUrl);
        JSONObject segments = JSON.parseObject(response);
        Set<String> segmentDimensions = new HashSet<>();
        Set<String> segmentMetrics = new HashSet<>();
        JSONObject meta;
        for (Object value : segments.values()) {
            for (Object v : ((JSONObject) value).values()) {
                meta = ((JSONObject) v).getJSONObject("metadata");
                for (String s : meta.getString("metrics").trim().split(",")) {
                    segmentMetrics.add(s.trim());
                }
                for (String s : meta.getString("dimensions").trim().split(",")) {
                    segmentDimensions.add(s.trim());
                }
            }
        }
        Set<String> oldVersionMetricSet = ImmutableSet.of("client_ip_sketch", "server_ip_sketch", "session_identifier_sketch", "in_latency_ms_sketch", "out_latency_ms_sketch", "latency_ms_sketch");
        boolean segmentNewVersion = segmentMetrics.stream().filter(x -> newVersionMetricSet.contains(x)).count() >= newVersionMetricSet.size()/2;
        boolean segmentOldVersion = segmentMetrics.stream().filter(x -> oldVersionMetricSet.contains(x)).count() >= oldVersionMetricSet.size()/2;
        if(!segmentOldVersion){
            return;
        }

        JSONObject oldParams = statisticsRuleOldDataParams();
        JSONArray oldDimensions = oldParams.getJSONObject("dimensionsSpec").getJSONArray("dimensions");
        JSONArray oldMetrics = oldParams.getJSONArray("metricsSpec");
        String name;
        if(segmentNewVersion){
            // 一部分新版本, 一部分旧版本
            for (int i = 0; i < oldDimensions.size(); i++) {
                item = oldDimensions.get(i);
                if(item instanceof String){
                    name = (String) item;
                }else{
                    name = ((JSONObject)item).getString("name");
                }
                if(!dimensions.contains(name) && segmentDimensions.contains(name)){
                    dimensionArray.add(item);
                }
            }
            for (int i = 0; i < oldMetrics.size(); i++) {
                item = oldMetrics.get(i);
                name = ((JSONObject)item).getString("name");
                if(!metrics.contains(name) && segmentMetrics.contains(name)){
                    metricArray.add(item);
                }
            }
        }else{
            // 全是旧版本
            dimensionArray.clear();
            metricArray.clear();
            for (int i = 0; i < oldDimensions.size(); i++) {
                item = oldDimensions.get(i);
                if(item instanceof String){
                    name = (String) item;
                }else{
                    name = ((JSONObject)item).getString("name");
                }
                if(segmentDimensions.contains(name)){
                    dimensionArray.add(item);
                }
            }
            for (int i = 0; i < oldMetrics.size(); i++) {
                item = oldMetrics.get(i);
                name = ((JSONObject)item).getString("name");
                if(segmentMetrics.contains(name)){
                    metricArray.add(item);
                }
            }
            //param.put("dimensionsSpec", oldParams.getJSONObject("dimensionsSpec"));
            //param.put("metricsSpec", oldParams.getJSONArray("metricsSpec"));
        }
    }

    private JSONObject statisticsRuleOldDataParams(){
        String params = "{\n" +
                "    \"dimensionsSpec\": {\n" +
                "        \"dimensions\": [\n" +
                "            \"device_id\",\n" +
                "            \"device_group\",\n" +
                "            \"data_center\",\n" +
                "            {\"name\": \"vsys_id\", \"type\": \"long\"},\n" +
                "            {\"name\": \"rule_id\", \"type\": \"long\"},\n" +
                "            {\"name\": \"template_id\", \"type\": \"long\"},\n" +
                "            {\"name\": \"chart_id\", \"type\": \"long\"},\n" +
                "            {\"name\": \"version\", \"type\": \"long\"},\n" +
                "            {\"name\": \"client_ip_object\", \"type\": \"string\", \"multiValueHandling\": \"SORTED_SET\"},\n" +
                "            {\"name\": \"server_ip_object\", \"type\": \"string\", \"multiValueHandling\": \"SORTED_SET\"},\n" +
                "            {\"name\": \"fqdn_category\", \"type\": \"string\", \"multiValueHandling\": \"SORTED_SET\"},\n" +
                "            \"client_ip\",\n" +
                "            \"server_ip\",\n" +
                "            \"client_country\",\n" +
                "            \"server_country\",\n" +
                "            {\"name\": \"client_asn\", \"type\": \"long\"},\n" +
                "            {\"name\": \"server_asn\", \"type\": \"long\"},\n" +
                "            \"server_fqdn\",\n" +
                "            \"server_domain\",\n" +
                "            \"application\",\n" +
                "            {\"name\": \"c2s_ttl\", \"type\": \"long\"},\n" +
                "            {\"name\": \"s2c_ttl\", \"type\": \"long\"},\n" +
                "            {\"name\": \"c2s_link_id\", \"type\": \"long\"},\n" +
                "            {\"name\": \"s2c_link_id\", \"type\": \"long\"},\n" +
                "            {\"name\": \"client_port\", \"type\": \"long\"},\n" +
                "            {\"name\": \"server_port\", \"type\": \"long\"}\n" +
                "        ]\n" +
                "    },\n" +
                "    \"metricsSpec\": [\n" +
                "        {\"type\": \"HLLDSketchMerge\", \"name\": \"session_identifier_sketch\", \"fieldName\": \"session_identifier_sketch\"},\n" +
                "        {\"type\": \"HLLDSketchMerge\", \"name\": \"client_ip_sketch\", \"fieldName\": \"client_ip_sketch\"},\n" +
                "        {\"type\": \"HLLDSketchMerge\", \"name\": \"server_ip_sketch\", \"fieldName\": \"server_ip_sketch\"},\n" +
                "        {\"type\": \"HdrHistogramSketchMerge\", \"name\": \"in_latency_ms_sketch\", \"fieldName\": \"in_latency_ms_sketch\"},\n" +
                "        {\"type\": \"HdrHistogramSketchMerge\", \"name\": \"out_latency_ms_sketch\", \"fieldName\": \"out_latency_ms_sketch\"},\n" +
                "        {\"type\": \"HdrHistogramSketchMerge\", \"name\": \"latency_ms_sketch\", \"fieldName\": \"latency_ms_sketch\"},\n" +
                "        {\"type\": \"longSum\", \"name\": \"in_bytes\", \"fieldName\": \"in_bytes\"},\n" +
                "        {\"type\": \"longSum\", \"name\": \"out_bytes\", \"fieldName\": \"out_bytes\"},\n" +
                "        {\"type\": \"longSum\", \"name\": \"bytes\", \"fieldName\": \"bytes\"},\n" +
                "        {\"type\": \"longSum\", \"name\": \"in_pkts\", \"fieldName\": \"in_pkts\"},\n" +
                "        {\"type\": \"longSum\", \"name\": \"out_pkts\", \"fieldName\": \"out_pkts\"},\n" +
                "        {\"type\": \"longSum\", \"name\": \"pkts\", \"fieldName\": \"pkts\"},\n" +
                "        {\"type\": \"longSum\", \"name\": \"new_c2s_flows\", \"fieldName\": \"new_c2s_flows\"},\n" +
                "        {\"type\": \"longSum\", \"name\": \"new_s2c_flows\", \"fieldName\": \"new_s2c_flows\"},\n" +
                "        {\"type\": \"longSum\", \"name\": \"new_in_sessions\", \"fieldName\": \"new_in_sessions\"},\n" +
                "        {\"type\": \"longSum\", \"name\": \"new_out_sessions\", \"fieldName\": \"new_out_sessions\"},\n" +
                "        {\"type\": \"longSum\", \"name\": \"sessions\", \"fieldName\": \"sessions\"},\n" +
                "        {\"type\": \"longSum\", \"name\": \"new_unestablished_sessions\", \"fieldName\": \"new_unestablished_sessions\"},\n" +
                "        {\"type\": \"longSum\", \"name\": \"syn_pkts\", \"fieldName\": \"syn_pkts\"},\n" +
                "        {\"type\": \"longSum\", \"name\": \"c2s_ip_fragments\", \"fieldName\": \"c2s_ip_fragments\"},\n" +
                "        {\"type\": \"longSum\", \"name\": \"s2c_ip_fragments\", \"fieldName\": \"s2c_ip_fragments\"},\n" +
                "        {\"type\": \"longSum\", \"name\": \"ip_fragments\", \"fieldName\": \"ip_fragments\"},\n" +
                "        {\"type\": \"HdrHistogramSketchMerge\", \"name\": \"in_pkt_length_sketch\", \"fieldName\": \"in_pkt_length_sketch\"},\n" +
                "        {\"type\": \"HdrHistogramSketchMerge\", \"name\": \"out_pkt_length_sketch\", \"fieldName\": \"out_pkt_length_sketch\"},\n" +
                "        {\"type\": \"HdrHistogramSketchMerge\", \"name\": \"pkt_length_sketch\", \"fieldName\": \"pkt_length_sketch\"}\n" +
                "    ]\n" +
                "}";
        return JSON.parseObject(params);
    }

    public boolean needFetchCompactMetaTimeTooLong(JSONObject param, int fileCount){
        return fileCount >= 2000 && needFetchCompactMeta(param);
    }

    public boolean needFetchCompactMeta(JSONObject param){
        if(param.get("dimensionsSpec") == null){
            return true;
        }

        if(param.get("metricsSpec") == null){
            return true;
        }

        if(param.get("granularitySpec") == null){
            return true;
        }else{
            JSONObject granularityParam = param.getJSONObject("granularitySpec");
            if(granularityParam.get("queryGranularity") == null){
                return true;
            }
            if(granularityParam.get("rollup") == null){
                return true;
            }
        }

        return false;
    }

    public void generateOverrideCompactTaskDefaultParam(JSONObject param){
        String dataSource = param.getString("dataSource");
        JSONObject defaultParam = extractCompactTaskDefaultParam(dataSource);
        Object dimensionsSpec = defaultParam.get("dimensionsSpec");
        Object metricsSpec = defaultParam.get("metricsSpec");
        JSONObject granularitySpec = (JSONObject) defaultParam.get("granularitySpec");
        if(!param.containsKey("dimensionsSpec") && dimensionsSpec != null){
            param.put("dimensionsSpec", dimensionsSpec);
        }
        if(!param.containsKey("metricsSpec") && metricsSpec != null){
            param.put("metricsSpec", metricsSpec);
        }
        if(granularitySpec != null){
            if(!param.containsKey("granularitySpec")){
                param.put("granularitySpec", new JSONObject());
            }
            JSONObject granularityParam = param.getJSONObject("granularitySpec");
            if(!granularityParam.containsKey("segmentGranularity") && granularitySpec.containsKey("segmentGranularity")){
                granularityParam.put("segmentGranularity", granularitySpec.get("segmentGranularity"));
            }
            if(!granularityParam.containsKey("queryGranularity") && granularitySpec.containsKey("queryGranularity")){
                granularityParam.put("queryGranularity", granularitySpec.get("queryGranularity"));
            }
            if(!granularityParam.containsKey("rollup") && granularitySpec.containsKey("rollup")){
                granularityParam.put("rollup", granularitySpec.get("rollup"));
            }
        }
    }

    public JSONObject extractCompactTaskDefaultParam(String dataSource){
        JSONObject defaultSpec = new JSONObject(8);

        try {
            String url = UrlUtil.getUrl(server, "/druid/indexer/v1/supervisor/" + dataSource);
            String response = HttpClientUtils.httpGet(url);
            JSONObject spec = JSON.parseObject(response);

            // 提取任务配置: 有多种配置方式
            String[] dimensionsPaths = new String[]{"$.dataSchema.dimensionsSpec", "$.spec.dataSchema.dimensionsSpec", "$.dataSchema.parser.parseSpec.dimensionsSpec"};
            String[] metricsPaths = new String[]{"$.dataSchema.metricsSpec", "$.spec.dataSchema.metricsSpec"};
            String[] granularityPaths = new String[]{"$.dataSchema.granularitySpec", "$.spec.dataSchema.granularitySpec"};
            JSONObject dimensionsSpec = null;
            JSONArray metricsSpec = null;
            JSONObject granularitySpec = null;
            JSONObject metric;
            String fieldName;
            String name;

            for (String dimensionsPath : dimensionsPaths) {
                dimensionsSpec = (JSONObject) JSONPath.of(dimensionsPath).eval(spec);
                if(dimensionsSpec != null){
                    break;
                }
            }
            for (String metricsPath : metricsPaths) {
                metricsSpec = (JSONArray) JSONPath.of(metricsPath).eval(spec);
                if(metricsSpec != null){
                    for (int i = 0; i < metricsSpec.size(); i++) {
                        metric = metricsSpec.getJSONObject(i);
                        fieldName = metric.getString("fieldName");
                        name = metric.getString("name");
                        if(fieldName != null && !fieldName.equals(name)){
                            metric.put("fieldName", name);
                        }
                    }
                    break;
                }
            }
            for (String granularityPath : granularityPaths) {
                granularitySpec = (JSONObject) JSONPath.of(granularityPath).eval(spec);
                if(granularitySpec != null){
                    break;
                }
            }

            defaultSpec.put("dimensionsSpec", dimensionsSpec);
            defaultSpec.put("metricsSpec", metricsSpec);
            defaultSpec.put("granularitySpec", granularitySpec);
        } catch (Exception e) {
            XxlJobLogger.log("extract compact task default param error:" + e.getMessage());
            XxlJobLogger.log(e);
        }

        return defaultSpec;
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

    public void deleteMetaDataAsync(List<JSONObject> submittedTaskParams, List<DruidWaitedSubmitTaskParam> waitedSubmitTaskParams){
        //默认查询任务状态限时8小时
        deleteMetaDataAsync(submittedTaskParams, Duration.ofHours(8), waitedSubmitTaskParams);
    }

    /**
     * 压缩任务通过http请求提交后，获取任务状态码，根据任务状态删除历史元数据
     */
    public void deleteMetaDataAsync(List<JSONObject> submittedTaskParams, Duration maxWaitDuration, List<DruidWaitedSubmitTaskParam> waitedSubmitTaskParams){
        final long endTime = System.currentTimeMillis() + maxWaitDuration.toMillis();
        final long maxWaitSubmitTime = System.currentTimeMillis() + maxWaitDuration.toMillis() * 2 / 3;
        XxlJobLogger.log("wait delete submitted tasks:" + String.join("\n", submittedTaskParams.stream().map(x -> x.toString()).collect(Collectors.toList())));
        if(waitedSubmitTaskParams.size() > 0){
            XxlJobLogger.log(StringUtils.repeat('-', 100));
            XxlJobLogger.log("wait submit tasks:" + String.join("\n", waitedSubmitTaskParams.stream().map(x -> x.param.toString()).collect(Collectors.toList())));
        }
        if(submittedTaskParams.isEmpty()){
            return;
        }

        new Thread(() -> {
            try {
                Thread.sleep(1000 * 40);
            }catch (Exception ex) {
            }
            logger.info("DruidCompactDeleteMetaDataThread start, wait delete size:" + submittedTaskParams.size() + ", wait submit size:" + waitedSubmitTaskParams.size());
            DruidCompactUtil druidCompactUtil = DruidCompactUtil.getInstance(server);
            while (submittedTaskParams.size() > 0 && System.currentTimeMillis() < endTime){
                try {
                    Iterator<JSONObject> iterator = submittedTaskParams.iterator();
                    int finishEdCnt = 0;
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
                            finishEdCnt ++;
                            Thread.sleep(1000 * 10);
                            String url = UrlUtil.getUrl(server, "/druid/indexer/v1/task");
                            String deleteJson = "{\"type\": \"kill\"," +
                                    "\"dataSource\"" + ":" + "\"" + dataSource + "\"," +
                                    "\"interval\"" + ":" + "\"" + interval + "\"}";
                            logger.info("deleteMetaData:" + deleteJson);
                            HttpClientUtils.httpPost(url, deleteJson);
                        }else if("FAILED".equals(statusCode)){
                            iterator.remove();
                            finishEdCnt ++;
                            logger.warn(String.format("task(%s) is failed:%s", taskId, response));
                        }
                    }

                    if(submittedTaskParams.isEmpty() && waitedSubmitTaskParams.size() > 0){
                        Thread.sleep(1000 * 90);
                    }

                    if(finishEdCnt > 0 && waitedSubmitTaskParams.size() > 0 && System.currentTimeMillis() < maxWaitSubmitTime){
                        int availableSlots = druidCompactUtil.queryAvailableSlots();
                        Iterator<DruidWaitedSubmitTaskParam> waitedParamIterator = waitedSubmitTaskParams.iterator();
                        while (waitedParamIterator.hasNext()) {
                            DruidWaitedSubmitTaskParam waitedParam = waitedParamIterator.next();
                            if(availableSlots >= waitedParam.maxNumConcurrentSubTasks){
                                availableSlots -= waitedParam.maxNumConcurrentSubTasks;
                                logger.info("submitCompactTask:" + waitedParam.param);
                                waitedParamIterator.remove();
                                druidCompactUtil.submitCompactTask(waitedParam.param, submittedTaskParams);
                            }else{
                                break;
                            }
                        }
                        logger.info("availableSlots:" + availableSlots);
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

    public int queryAvailableSlots(){
        String url = UrlUtil.getUrl(server, "/druid/indexer/v1/workers");
        String response = HttpClientUtils.httpGet(url);
        JSONArray rsts = JSON.parseArray(response);

        int totalSlots = rsts.size() * 55;
        int usedSlots = 0;
        for (int i = 0; i < rsts.size(); i++) {
            Integer currCapacityUsed = rsts.getJSONObject(i).getInteger("currCapacityUsed");
            if (currCapacityUsed != null) {
                usedSlots += currCapacityUsed;
            } else {
                usedSlots += rsts.getJSONObject(i).getJSONArray("runningTasks").size();
            }
        }

        return totalSlots > usedSlots? totalSlots - usedSlots: 0;
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
            if(StringUtils.isBlank(queryGranularity)){
                return false;
            }else{
                return true;
            }
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
            granularity = mapper.readValue(str, Granularity.class);
        }else{
            granularity = Granularity.fromString(str);
        }

        return granularity;
    }
}
