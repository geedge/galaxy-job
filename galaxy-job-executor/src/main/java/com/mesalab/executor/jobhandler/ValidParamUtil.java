package com.mesalab.executor.jobhandler;

import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.geedgenetworks.utils.StringUtil;
import com.xxl.job.core.log.XxlJobLogger;

import java.util.List;
import java.util.Map;

public class ValidParamUtil {

    public static List<Map> parserParams(String params) {
        if (StrUtil.isBlank(params)) {
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
    public static List<Map> parserParamsSink(String params) {
        if (StrUtil.isBlank(params)) {
            XxlJobLogger.log("params is empty!");
            return null;
        }
        List<Map> paramList = JSONUtil.toList(params, Map.class);
        for (Map<String, Map<String, String>> param : paramList) {
            if (ObjectUtil.isNull(param)) {
                XxlJobLogger.log("params is not format json ! ");
                return null;
            }
            if (ObjectUtil.isNull(param.get("sink"))) {
                XxlJobLogger.log("params sink is null ! ");
                return null;
            }
        }
        return paramList;
    }

    public static Map<String, Object> parseParams(String params) {

        if (StringUtil.isBlank(params)) {
            XxlJobLogger.log("params is Empty !");
            return null;
        }
        Map<String, Object> paramMap = JSONUtil.toBean(params, Map.class);
        if (paramMap == null) {
            XxlJobLogger.log("params error !");
            return null;
        }
        return paramMap;
    }
}
