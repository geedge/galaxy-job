package com.mesalab.executor.jobhandler;

import cn.hutool.core.util.ObjectUtil;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import com.mesalab.executor.service.DataSinkService;
import com.mesalab.executor.service.DataSourceService;
import com.mesalab.executor.service.DataTransformService;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import com.zdjizhi.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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
     * {
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
     * }
     *
     * @param params
     */
    @XxlJob("dataExtractJobHandler")
    public ReturnT<String> dataExtract(String params) {
        try {
            Map<String, Map<String, String>> paramsMap = parserParams(params);
            if (ObjectUtil.isEmpty(paramsMap)) {
                logger.error("params parser error , params is {}", params);
                return IJobHandler.FAIL;
            }
            logger.info("query params is {}", params);

            Map<String,Object> sourceResult = dataSourceService.adapt(paramsMap.get("source"));
            if (ObjectUtil.isNotEmpty(paramsMap.get("transform"))) {
                Map<String, Object> transformResult = dataTransformService.adapt(paramsMap.get("transform"), sourceResult);
                dataSinkService.adapt(transformResult, paramsMap.get("sink"));
            } else {
                dataSinkService.adapt(sourceResult, paramsMap.get("sink"));
            }
            return ReturnT.SUCCESS;
        } catch (Exception e) {
            e.printStackTrace();
            XxlJobLogger.log(e.getMessage());
            logger.error(e.getMessage());
            return ReturnT.FAIL;
        }

    }

    private Map<String, Map<String, String>> parserParams(String params) {
        if (StringUtil.isBlank(params)) {
            XxlJobLogger.log("params is empty!");
            return null;
        }
        Map<String, Map<String, String>> param = JSONUtil.toBean(params, Map.class);
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
        return param;
    }
}
