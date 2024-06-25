package com.mesalab.executor.jobhandler;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.mesalab.executor.core.utils.JobUtil;
import com.mesalab.executor.core.utils.TypeUtils;
import com.mesalab.executor.pojo.JDBCParam;
import com.mesalab.executor.service.StorageQuotaInfoService;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.XxlJob;
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
    private StorageQuotaInfoService storageQuotaInfoService;


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

                failCount += storageQuotaInfoService.getAndSaveStorageQuotaInfo(items, jdbcParam, containBytes);

            }
        } catch (Exception e) {
            failCount++;
            JobUtil.errorLog("getDatabaseStorageQuota job error:", e.getMessage());
        }
        return failCount > 0 ? ReturnT.FAIL : ReturnT.SUCCESS;
    }
}
