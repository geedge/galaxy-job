package com.mesalab.executor.jobhandler;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.mesalab.executor.core.utils.JobUtil;
import com.mesalab.executor.core.utils.TypeUtils;
import com.mesalab.executor.pojo.JDBCParam;
import com.mesalab.executor.service.StorageQuotaService;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

/**
 * @Description:
 * @Author: zhq
 * @CreateDate: 2024/6/13
 * @Version: 1.0
 */

@Component
public class StorageQuotaJob {

    @Resource
    private StorageQuotaService storageQuotaService;


    @XxlJob("getDatabaseStorageQuotaJobHandler")
    public ReturnT<String> getDatabaseStorageQuotaJobHandler(String params) {

        int failCount = 0;
        try {
            List<Map> paramsMaps = ValidParamUtil.parserParamsSink(params);
            for (Map paramsMap : paramsMaps) {
                Map<String, Object> source = BeanUtil.beanToMap(paramsMap.get("source"));
                String[] items = StrUtil.split(source.get("items").toString(), ",");
                Boolean containBytes = TypeUtils.castToBoolean(source.get("containBytes"));
                JDBCParam jdbcParam = BeanUtil.toBean(paramsMap.get("sink"), JDBCParam.class);

                failCount += storageQuotaService.getAndSaveStorageQuotaInfo(items, jdbcParam, containBytes);

            }
        } catch (Exception e) {
            failCount++;
            JobUtil.errorLog("getDatabaseStorageQuota job error:", e.getMessage());
        }
        return failCount > 0 ? ReturnT.FAIL : ReturnT.SUCCESS;
    }

    @XxlJob("deleteOldLogJobHandler")
    public ReturnT<String> deleteOldLogJobHandler(String params) {

        int failCount = 0;
        try {
            Map<String, Object> paramsMap = ValidParamUtil.parseParams(params);
            if (ObjectUtils.isEmpty(paramsMap)) {
                JobUtil.errorLog("params parse error , params is {}", params);
                return IJobHandler.FAIL;
            }
            String logType = TypeUtils.castToString(paramsMap.get("logType"));
            int durationMinutes = TypeUtils.castToInt(paramsMap.get("durationMinutes"));
            int minIntervalMinutes = TypeUtils.castToInt(paramsMap.get("minIntervalMinutes"));
            JDBCParam jdbcParam = JSONUtil.toBean((JSONObject) paramsMap.get("jdbcParam"), JDBCParam.class);
            failCount += storageQuotaService.deleteOldLog(logType, durationMinutes, minIntervalMinutes, jdbcParam);

        } catch (Exception e) {
            failCount++;
            JobUtil.errorLog("getDatabaseStorageQuota job error:", e.getMessage());
        }
        return failCount > 0 ? ReturnT.FAIL : ReturnT.SUCCESS;
    }

}
