package com.mesalab.executor.jobhandler.knowledgeBase;

import com.mesalab.executor.core.utils.JobUtil;
import com.mesalab.executor.jobhandler.ValidParamUtil;
import com.mesalab.executor.service.DataSourceService;
import com.mesalab.executor.service.knowledgeBase.KnowledgeBaseService;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.XxlJob;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

@Component
public class KnowledgeBaseJob {

    @Resource
    DataSourceService dataSourceService;
    @Resource
    KnowledgeBaseService knowledgeBaseService;

    /**
     * [{
     * "source": {
     *   "type": "mariadb",
     *   "ip": "*.*.*.*",
     *   "database": "",
     *   "username": "",
     *   "pin": "",
     *   "sql": "SELECT * FROM cn_domain_tag_user_defined"
     * },
     * "transform": {},
     * "sink": {
     *   "type": "http_binary",
     *   "url": "http://IP:9999/knowledge_base/v1",
     *   "params": {"name": "domain",format:"csv",type:"dns"}
     * }
     * }]
     * @param params
     * @return
     */
    @XxlJob("knowledgeBaseJob")
    public ReturnT<String> knowledgeBaseJob(String params) {

        //1. 通用查询数据库模块，查询版本控制表，判断是否更新
        //2. 通用格式数据mysql-> mmdb,csv,json
        //3. 通用数据转存文件流模块
        //4. 通用调用kb api上传接口模块
        //5. 通用调用kb api 查询模块

        List<Map> paramsMaps = ValidParamUtil.parserParams(params);
        for (Map<String, Map> paramsMap : paramsMaps) {
            try {
                Map<String, Object> sourceData = dataSourceService.adapt(paramsMap.get("source"));
                knowledgeBaseService.pushKnowledgeBase(sourceData, paramsMap.get("sink"));
            } catch (Exception e) {
                JobUtil.errorLog(e);
                return ReturnT.FAIL;
            }
        }
        return ReturnT.SUCCESS;
    }

    void checkStatus() {
        //1. 查询数据库是否要更新，哪些需要更新

    }
}
