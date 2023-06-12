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

    private static final String KNOWLEDGE_BASE_META_SQL = "SELECT a.knowledge_id knowledge_id,name,version,source FROM cn_knowledge_base_sync_version a LEFT JOIN cn_knowledge_base b ON a.knowledge_id=b.knowledge_id WHERE `status`=1";
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
        List<Map> paramsMaps = ValidParamUtil.parserParams(params);
        int failedTask = 0;
        for (Map<String, Map> paramsMap : paramsMaps) {
            try {
                Map source = paramsMap.get("source");
                Map sink = paramsMap.get("sink");
                List<Map> sinkParamFormat = getSinkParamFormat(sink);
                List<Map> dbKnowledgeMeta = getKnowledgeMeta(source);
                for (Map dbKm : dbKnowledgeMeta) {
                    try {
                        if (metadataNoChanged(dbKm)) {
                            continue;
                        }
                        for (Map sinkParam : sinkParamFormat) {
                            if (sinkParam.get("name").equals(dbKm.get("source"))) {
                                //1.mmdb格式调度perl脚本，2.其它格式
                                boolean hasPush = false;
                                if ("mmdb".equals(sinkParam.get("format"))) {
                                    if (!String.valueOf(dbKm.get("knowledge_id")).equals(String.valueOf(sinkParam.get("id")))) {
                                        continue;
                                    }
                                    hasPush = knowledgeBaseService.mmdbBuildPush(builtParam(sink, dbKm, sinkParam));
                                } else {
                                    source.put("sql", MessageFormat.format(KNOWLEDGE_BASE_SQL, dbKm.get("source"), dbKm.get("knowledge_id")));
                                    //推送到知识库接口
                                    hasPush = knowledgeBaseService.commonPushFile(source, builtParam(sink, dbKm, sinkParam));
                                }
                                String kmMeta = JSONUtil.toJsonStr(dbKm);
                                if (hasPush) {
                                    knowledgeBaseService.modifyMetadata(String.valueOf(dbKm.get("knowledge_id")), kmMeta);
                                    JobUtil.infoLog("knowledgeBase push success, metadata : {}  ", kmMeta);
                                } else {
                                    failedTask++;
                                    JobUtil.infoLog("knowledgeBase push fail, metadata : {}  ", kmMeta);
                                }
                            }
                        }
                    } catch (Exception e) {
                        failedTask++;
                        JobUtil.errorLog(e);
                    }
                }

            } catch (Exception e) {
                JobUtil.errorLog(e);
                return ReturnT.FAIL;
            }
        }
        if (failedTask > 0) {
            return ReturnT.FAIL;
        }
        return ReturnT.SUCCESS;
    }

    private Map builtParam(Map sink, Map dbKm, Map sinkParam) {
        HashMap<String, Object> newParam = new HashMap<>();
        newParam.put("id", dbKm.get("knowledge_id"));
        newParam.put("name", dbKm.get("name"));
        newParam.put("format", sinkParam.get("format"));
        newParam.put("type", sinkParam.get("type"));
        newParam.put("isValid", 1);
        sink.put("params", newParam);
        return sink;
    }
    /**
     * 版本是否变化
     *
     * @param km
     * @return
     */
    private boolean metadataNoChanged(Map km) {
        String knowledgeBaseMetadata = knowledgeBaseService.getMetadata(String.valueOf(km.get("knowledge_id")));
        if (StrUtil.isNotEmpty(knowledgeBaseMetadata)) {
            Map historyMetadata = JSONUtil.toBean(knowledgeBaseMetadata, Map.class);
            if (historyMetadata.get("version").equals(km.get("version"))) {
                JobUtil.infoLog("knowledgeBase version no change {}", knowledgeBaseMetadata);
                return true;
            }
        }
        return false;
    }

    /**
     * 匹配配置信息执行
     * <p>
     * k->表名#type
     * v->params
     *
     * @param sink
     * @return
     */
    private List<Map> getSinkParamFormat(Map sink) {
        JSONArray sinkParamsArr = JSONUtil.parseArray(sink.get("params"));
        List<Map> sinkParams = JSONUtil.toList(sinkParamsArr, Map.class);
//        //获取设置的格式
//        Map<Object, Map> sinkMap = new HashMap<>();
//        for (Map sinkParam : sinkParams) {
//            sinkMap.put(sinkParam.get("name") + "#" + sinkParam.get("type"), sinkParam);
//        }
        return sinkParams;
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
        JobUtil.infoLog("get database knowledge metadata {}", JSONUtil.toJsonPrettyStr(kbMetas));
        return kbMetas;
    }

}
