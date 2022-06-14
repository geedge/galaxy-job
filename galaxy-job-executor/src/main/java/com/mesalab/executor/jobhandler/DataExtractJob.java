package com.mesalab.executor.jobhandler;

import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import com.mesalab.executor.service.DataSinkService;
import com.mesalab.executor.service.DataSourceService;
import com.mesalab.executor.service.DataTransformService;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.core.util.ShardingUtil;
import com.zdjizhi.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @description:
 * @author: zhq
 * @create: 2022-03-29
 **/

@Component
public class DataExtractJob {

    @Autowired
    DataSourceService dataSourceService;
    @Autowired
    DataTransformService dataTransformService;
    @Autowired
    DataSinkService dataSinkService;

    private Log logger = Log.get();

    /**
     * [{
     * "source": {
     * "type": "file",
     * "path": "/",
     * "fileType": "json,xlsx",
     * "source_table": "ioc"
     * },
     * "transform": {"type": "websketch"},
     * "sink": {
     * "type": "mariadb",
     * "ip": "192.168.44.*",
     * "database": "web_skecth",
     * "table": "ioc_malware",
     * "username": "root",
     * "pin": ""
     * }
     * }]
     *
     * @param params
     */
    @XxlJob("dataExtractJobHandler")
    public ReturnT<String> dataExtract(String params) {
        try {
            logger.info("query params is {}", params);
            List<Map> paramsMaps = parserParams(params);
            if (ObjectUtil.isEmpty(paramsMaps)) {
                logger.error("params parser error , params is {}", params);
                return IJobHandler.FAIL;
            }
            for (Map<String, Map<String, String>> paramsMap : paramsMaps) {
                Map<String, Object> sourceResult = dataSourceService.adapt(paramsMap.get("source"));
                if (ObjectUtil.isNotEmpty(paramsMap.get("transform"))) {
                    Map<String, Object> transformResult = dataTransformService.adapt(paramsMap.get("transform"), sourceResult);
                    dataSinkService.adapt(transformResult, paramsMap.get("sink"));
                } else {
                    dataSinkService.adapt(sourceResult, paramsMap.get("sink"));
                }
            }

            return ReturnT.SUCCESS;
        } catch (Exception e) {
            XxlJobLogger.log(e.getMessage());
            logger.error(e.getMessage());
            return ReturnT.FAIL;
        }

    }

    @XxlJob("dataShardingExtractJobHandler")
    public ReturnT<String> dataShardingExtract(String params) {

        try {
            ShardingUtil.ShardingVO shardingVo = ShardingUtil.getShardingVo();
            int shardingTotal = shardingVo.getTotal();//执行器总数
            int shardingIndex = shardingVo.getIndex();//当前执行器编号
            XxlJobLogger.log("sharding params, sharding number = {}, total sharding count  = {}", shardingVo.getIndex(), shardingVo.getTotal());
            logger.info("sharding params, sharding number = {}, total sharding count  = {}", shardingVo.getIndex(), shardingVo.getTotal());

            logger.info("query params is {}", params);
            List<Map> paramsMaps = parserParams(params);
            if (ObjectUtil.isEmpty(paramsMaps)) {
                logger.error("params parser error , params is {}", params);
                return IJobHandler.FAIL;
            }
            for (Map<String, Map<String, String>> paramsMap : paramsMaps) {
                long start = System.currentTimeMillis();
                Map<String, String> soureParams = paramsMap.get("source");
                Map<String, String> transformParams = paramsMap.get("transform");
                Map<String, String> sinkParams = paramsMap.get("sink");

                Map<String, String> countParams = new HashMap<>();
                countParams.putAll(soureParams);
                countParams.put("sql", "select count(*) AS count from " + countParams.get("table"));
                List<Map> countMap = dataSourceService.mysqlSource(countParams);
                String parentSql = soureParams.get("sql");
                int totalCount = Integer.valueOf(StrUtil.toString(countMap.get(0).get("count")));
                //分片任务平均分配,每个执行器处理数据总量 [index*avgCount,(index+1)*avgCount]
                int avgCount = totalCount % shardingTotal == 0 ? totalCount / shardingTotal : totalCount / shardingTotal + 1;
                int shardingStart = shardingIndex * avgCount;
                int shardingEnd = (shardingIndex + 1) * avgCount;

                int pageCount = Integer.valueOf(soureParams.get("sharding"));
                int pageSize = avgCount % pageCount == 0 ? avgCount / pageCount : avgCount / pageCount + 1;
                logger.info("total data {},current sharding {},process data size {} and data rang [{},{}]. paging count {} ,paging size {}.", totalCount, shardingIndex, avgCount, shardingStart, shardingEnd, pageCount, pageSize);
                XxlJobLogger.log("total data {},current sharding {},process data size {} and data rang [{},{}]. paging count {} ,paging size {}.", totalCount, shardingIndex, avgCount, shardingStart, shardingEnd, pageCount, pageSize);

                for (int i = 0; i < pageCount; i++) {
                    int pageNum = i + 1;
                    //查询数据
                    logger.info("sharding {} , paging {} source execute start", shardingIndex, pageNum);
                    XxlJobLogger.log("sharding {} ,paging {} source execute start", shardingIndex, pageNum);
                    String pageSql = parentSql + " order by id limit " + (shardingStart + i * pageSize) + "," + (pageSize > shardingEnd ? shardingEnd : pageSize);
                    soureParams.put("sql", pageSql);
                    List<Map> sourceResult = dataSourceService.mysqlSource(soureParams);
                    logger.info("sharding {} ,paging {} source execute end", shardingIndex, pageNum);
                    XxlJobLogger.log("sharding {} ,paging {} source execute end", shardingIndex, pageNum);
                    //处理数据
                    logger.info("sharding {} ,paging {} transform execute start", shardingIndex, pageNum);
                    XxlJobLogger.log("sharding {} ,paging {} transform execute start", shardingIndex, pageNum);
                    List<Map> transformResult = dataTransformService.isFqdnAccessible(sourceResult, transformParams);
                    logger.info("sharding {} ,paging {} transform execute end", shardingIndex, pageNum);
                    XxlJobLogger.log("sharding {} ,paging {} transform execute end", shardingIndex, pageNum);
                    sourceResult = null;
                    //数据入库
                    logger.info("sharding {} ,paging {} sink execute start", shardingIndex, pageNum);
                    XxlJobLogger.log("sharding {} ,paging {} sink execute start", shardingIndex, pageNum);
                    dataSinkService.exec(transformResult, sinkParams);
                    logger.info("sharding {} ,paging {} sink execute end", shardingIndex, pageNum);
                    XxlJobLogger.log("sharding {} ,paging {} sink execute end", shardingIndex, pageNum);
                    transformResult = null;
                }
                logger.info("sharding {} task end, take time {}ms", shardingIndex, System.currentTimeMillis() - start);
                XxlJobLogger.log("sharding {} task end, take time {}ms", shardingIndex, System.currentTimeMillis() - start);
            }

            return ReturnT.SUCCESS;
        } catch (Exception e) {
            XxlJobLogger.log(e.getMessage());
            logger.error(e);
            return ReturnT.FAIL;
        }

    }

    private List<Map> parserParams(String params) {
        if (StringUtil.isBlank(params)) {
            XxlJobLogger.log("params is empty!");
            return null;
        }
        List<Map> paramList = JSONUtil.toList(params, Map.class);
        for (Map<String, Map<String, String>> param : paramList) {
            if (ObjectUtil.isNull(param)) {
                XxlJobLogger.log("params is not format json ! ");
                return null;
            }
            if (ObjectUtil.isEmpty(param.get("source"))) {
                XxlJobLogger.log("source is empty ! ");
                return null;
            }
            if (ObjectUtil.isNull(param.get("transform"))) {
                XxlJobLogger.log("transform is null ! ");
                return null;
            }
            if (ObjectUtil.isNull(param.get("sink"))) {
                XxlJobLogger.log("sink is null ! ");
                return null;
            }
        }
        return paramList;
    }
}
