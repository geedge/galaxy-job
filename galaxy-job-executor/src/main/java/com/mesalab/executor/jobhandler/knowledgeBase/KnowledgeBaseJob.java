package com.mesalab.executor.jobhandler.knowledgeBase;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONUtil;
import com.mesalab.executor.core.utils.JobUtil;
import com.mesalab.executor.jobhandler.ValidParamUtil;
import com.mesalab.executor.service.DataSourceService;
import com.mesalab.executor.service.knowledgeBase.KnowledgeBaseService;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.XxlJob;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class KnowledgeBaseJob {

    @Resource
    DataSourceService dataSourceService;
    @Resource
    KnowledgeBaseService knowledgeBaseService;

    private static final String KNOWLEDGE_BASE_META_SQL = "SELECT a.knowledge_id id,name,version,`status` isValid,source FROM cn_knowledge_base_sync_version a LEFT JOIN cn_knowledge_base b ON a.knowledge_id=b.knowledge_id WHERE `status`=1";
    private String KNOWLEDGE_BASE_SQL = "SELECT * FROM {0} WHERE knowledge_id={1}";

    /**
     * [{
     * "source": {
     * "type": "mariadb",
     * "ip": "*.*.*.*",
     * "database": "",
     * "username": "",
     * "pin": ""
     * },
     * "transform": {},
     * "sink": {
     * "type": "http_binary",
     * "url": "http://IP:9999/knowledge_base/v1",
     * "params": [{"name": "domain",format:"csv",type:"dns"}]
     * }
     * }]
     *
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
                Map source = paramsMap.get("source");
                Map sink = paramsMap.get("sink");
                Map<Object, Map> sinkParamFormatMap = getSinkParamFormat(sink);
                List<Map> knowledgeMeta = getKnowledgeMeta(source);
                for (Map dbKm : knowledgeMeta) {
                    Map sinkParam = sinkParamFormatMap.get(dbKm.get("source"));
                    if (sinkParam == null || metadataNoChanged(dbKm)) {
                        continue;
                    }
                    dbKm.put("format", sinkParam.get("format"));
                    dbKm.put("type", sinkParam.get("type"));
                    sink.put("params", dbKm);
                    JobUtil.infoLog("knowledgeBase config info : {}", dbKm);
                    //1.mmdb格式调度perl脚本，2.其它格式

                    boolean hasPush = false;
                    if ("mmdb".equals(sinkParam.get("format"))) {
                        //1.脚本，2.状态
                        hasPush = knowledgeBaseService.mmdbBuildPush(sink);
                    } else {
                        source.put("sql", MessageFormat.format(KNOWLEDGE_BASE_SQL, dbKm.get("source"), dbKm.get("id")));
                        Map sourceData = dataSourceService.adapt(source);
                        JobUtil.infoLog("knowledgeBase id : {} query success", dbKm.get("id"));
                        //推送到知识库接口
                        hasPush = knowledgeBaseService.push(sourceData, sink);
                    }
                    String kmMeta = JSONUtil.toJsonStr(dbKm);
                    if (hasPush) {
                        knowledgeBaseService.modifyMetadata(String.valueOf(dbKm.get("id")), kmMeta);
                        JobUtil.infoLog("knowledgeBase push success, metadata : {}  ", kmMeta);
                    } else {
                        JobUtil.infoLog("knowledgeBase push fail, metadata : {}  ", kmMeta);
                    }
                }

            } catch (Exception e) {
                JobUtil.errorLog(e);
                return ReturnT.FAIL;
            }
        }
        return ReturnT.SUCCESS;
    }

    /**
     * 版本是否变化
     *
     * @param km
     * @return
     */
    private boolean metadataNoChanged(Map km) {
        String knowledgeBaseMetadata = knowledgeBaseService.getMetadata(String.valueOf(km.get("id")));
        if (StrUtil.isNotEmpty(knowledgeBaseMetadata)) {
            Map historyMetadata = JSONUtil.toBean(knowledgeBaseMetadata, Map.class);
            if (historyMetadata.get("version").equals(km.get("version"))) {
                JobUtil.infoLog("knowledgeBase meta no change {}", knowledgeBaseMetadata);
                return true;
            }
        }
        return false;
    }

    /**
     * 匹配配置信息执行
     *
     * @param sink
     * @return
     */
    private Map<Object, Map> getSinkParamFormat(Map sink) {
        JSONArray sinkParamsArr = JSONUtil.parseArray(sink.get("params"));
        List<Map> sinkParams = JSONUtil.toList(sinkParamsArr, Map.class);
        //获取设置的格式
        Map<Object, Map> sinkMap = new HashMap<>();
        for (Map sinkParam : sinkParams) {
            sinkMap.put(sinkParam.get("name"), sinkParam);
        }
        return sinkMap;
    }

    /**
     * 元数据信息
     *
     * @param source
     * @return
     * @throws Exception
     */
    private List<Map> getKnowledgeMeta(Map source) throws Exception {
        source.put("sql", KNOWLEDGE_BASE_META_SQL);
        List<Map> kbMetas = dataSourceService.mysqlSource(source);
        JobUtil.infoLog("get database knowledge metadata {}", kbMetas);
        return kbMetas;
    }

}
