package com.mesalab.executor.service.knowledgeBase;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.file.FileWriter;
import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.digest.DigestUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONPath;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.core.utils.JobUtil;
import com.mesalab.executor.pojo.KnowledgeBase;
import com.zdjizhi.utils.StringUtil;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.mesalab.executor.core.utils.Constant.WORK_PATH;

@Service
public class KnowledgeBaseService {

    /**
     * 上传文件到知识库
     *
     * @param sourceData
     * @param sinkParam
     * @throws Exception
     */
    public void pushKnowledgeBase(Map<String, Object> sourceData, Map<String, Object> sinkParam) throws Exception {
        String url = String.valueOf(sinkParam.get("url"));
        String params = String.valueOf(sinkParam.get("params"));
        JSONObject jsonObject = JSONUtil.parseObj(params);
        KnowledgeBase knowledgeBase = JSONUtil.toBean(jsonObject, KnowledgeBase.class);

        //1. 数据转为csv 临时文件 2. 判断是否有变更，csv存入上传知识库
        String tmpDir = StrUtil.join(File.separator, WORK_PATH, "tmp", UUID.randomUUID());
        try {
            List<Map> dataMap = (List<Map>) JobUtil.getFirstNotNull(sourceData);
            File tmpFile = new File(StringUtil.join(tmpDir, File.separator, knowledgeBase.getName(), ".", knowledgeBase.getFormat()));
            if ("json".equals(knowledgeBase.getFormat())) {
                FileWriter fileWriter = new FileWriter(tmpFile.getAbsolutePath());
                fileWriter.write(JSON.toJSONString(dataMap, SerializerFeature.PrettyFormat));
            } else {
                JobUtil.createCsvFile(dataMap, tmpFile.getAbsolutePath());
            }

            String newFileSha256 = DigestUtil.sha256Hex(tmpFile);
            String historySha256 = historyKnowledgeSha256(url).get(knowledgeBase.getName() + knowledgeBase.getFormat());
            if (!newFileSha256.equals(historySha256)) {
                knowledgeBase.setSha256(newFileSha256);
                String resultStr = HttpClientUtils.httpPostFile(url, tmpFile, BeanUtil.beanToMap(knowledgeBase));
                JobUtil.infoLog("knowledgeBase name {} has been pushed, result {}", knowledgeBase.getName(), resultStr);
            } else {
                JobUtil.infoLog("knowledgeBase name {} have not changed and are not pushed ,new file sha256 {}, history file sha256 {}", knowledgeBase.getName(), newFileSha256, historySha256);
            }
        } catch (Exception e) {
            JobUtil.errorLog(e);
            throw e;
        } finally {
            FileUtil.del(tmpDir);
        }

    }

    /**
     * 查询知识库最近版本
     */
    public Map<String, String> historyKnowledgeSha256(String url) {
        String result = HttpClientUtils.httpGet(url);
        Object extract = JSONPath.extract(result, "$.data[version=='latest']");
        JSONArray objects = JSONUtil.parseArray(extract);
        List<KnowledgeBase> knowledgeBases = JSONUtil.toList(objects, KnowledgeBase.class);
        return knowledgeBases.stream().collect(Collectors.toMap(
                kb -> kb.getName() + kb.getFormat(),
                KnowledgeBase::getSha256
        ));

    }
}
