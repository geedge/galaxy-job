package com.mesalab.executor.service.knowledgeBase;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.file.FileWriter;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONPath;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.nacos.api.config.annotation.NacosValue;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.core.utils.JobUtil;
import com.mesalab.executor.core.utils.ZookeeperUtils;
import com.mesalab.executor.pojo.KnowledgeBase;
import com.mesalab.executor.pojo.MMDBStatus;
import com.zdjizhi.utils.IpLookupV2;
import com.zdjizhi.utils.StringUtil;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.mesalab.executor.core.utils.Constant.*;

@Service
public class KnowledgeBaseService {


    @NacosValue(value = "${zookeeper.server}", autoRefreshed = true)
    private String zookeeperServer;

    /**
     * 上传文件到知识库
     *
     * @param sourceData
     * @param sinkParam
     * @throws Exception
     */
    public boolean push(Map<String, Object> sourceData, Map<String, Object> sinkParam) throws Exception {
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
            String resultStr = HttpClientUtils.httpPostFile(url, tmpFile, BeanUtil.beanToMap(knowledgeBase));
            JobUtil.infoLog("knowledgeBase name {} has been pushed, result {}", knowledgeBase.getName(), resultStr);
            if (!"-1".equals(resultStr)) {
                Map<String, Object> result = JSONUtil.toBean(resultStr, Map.class);
                if (result.get("status") != null && "200".equals(result.get("status"))) {
                    JobUtil.infoLog("ip locate knowledgeBase push success");
                    return true;
                }
            } else {
                JobUtil.errorLog("ip locate knowledgeBase push fail");
            }
        } catch (Exception e) {
            JobUtil.errorLog(e);
            throw e;
        } finally {
            FileUtil.del(tmpDir);
        }
        return false;
    }

    /**
     *
     */
    public boolean mmdbBuildPush(Map<String, Object> sinkParam) throws Exception {
        final String workDirCmd = "docker inspect --format '{{range.Mounts}}  {{.Source}} {{end}}' galaxy-job-executor |xargs -n 1|grep plugins";
        String url = String.valueOf(sinkParam.get("url"));
        String params = String.valueOf(sinkParam.get("params"));
        JSONObject jsonObject = JSONUtil.parseObj(params);
        KnowledgeBase knowledgeBase = JSONUtil.toBean(jsonObject, KnowledgeBase.class);

        try {
            String workDir = JobUtil.exeCmd(workDirCmd);
            JobUtil.infoLog("get docker work dir {}", workDir);
            String cmd = "docker run --rm -v " + workDir + "/mmdb:/mmdb --name perl-mmdb perl:v3.1 sh -c 'cd /mmdb && perl mmdb-perl-built-in.pl " + knowledgeBase.getId() + "'";
            JobUtil.infoLog("built docker perl cmd {}", cmd);

            int executeResult = JobUtil.executeCommand(cmd);
            JobUtil.infoLog("ip locate knowledgeBase perl execute result {}", executeResult == 0 ? "success" : "fail");
            String filename = StringUtil.join(knowledgeBase.getId(), ".", knowledgeBase.getFormat());

            if (executeResult == 0 && checkIpMMDBSample(KNOWLEDGE_WORK_DIR, filename)) {
                JobUtil.infoLog("ip locate knowledgeBase built success");
                //push knowledge
                File file = new File(StringUtil.join(KNOWLEDGE_WORK_DIR, File.separator, filename));

                String resultStr = HttpClientUtils.httpPostFile(url, file, BeanUtil.beanToMap(knowledgeBase));
                JobUtil.infoLog("ip locate knowledgeBase push knowledge result: {},\n message {}", knowledgeBase, resultStr);
                if (!"-1".equals(resultStr)) {
                    Map<String, Object> result = JSONUtil.toBean(resultStr, Map.class);
                    if (result.get("status") != null && "200".equals(result.get("status"))) {
                        JobUtil.infoLog("ip locate knowledgeBase push success");
                        return true;
                    }
                } else {
                    JobUtil.errorLog("ip locate knowledgeBase push fail");
                }
            }
        } catch (Exception e) {
            JobUtil.errorLog(e);
            throw e;
        }
        return false;
    }

    public String getMetadata(String knowledgeId) {
        try {
            ZookeeperUtils zk = new ZookeeperUtils();
            String nodeData = zk.getNodeData(ZK_KNOWLEDGE_PATH + knowledgeId, zookeeperServer);
            return nodeData;
        } catch (Exception e) {
            JobUtil.errorLog("knowledgeBase query zookeeper error :{}", e);
            return null;
        }
    }

    public void modifyMetadata(String knowledgeId, String data) {
        ZookeeperUtils zk = new ZookeeperUtils();
        //是否存在 不存在创建
        zk.modifyNode(ZK_KNOWLEDGE_PATH + knowledgeId, data, zookeeperServer);
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

    /**
     * 检查脚本运行结果和抽样ip是否打到包里
     *
     * @param
     * @return
     */
    public static boolean checkIpMMDBSample(String path, String filename) throws Exception {

        String builtStatusFile = "built-status.json";

        MMDBStatus builtMMDBStatus = JobUtil.readJsonFile(path + builtStatusFile, MMDBStatus.class);
        JobUtil.infoLog("built ip locate library result: status {}, message {}", builtMMDBStatus.getStatus(), builtMMDBStatus.getMessage());
        if (200 != builtMMDBStatus.getStatus()) {
            JobUtil.errorLog("built status fail {}", builtMMDBStatus.getStatus());
            return false;
        }
        //测试抽样数据能查到结果说明打包成功
        IpLookupV2 ipLookup = new IpLookupV2.Builder(false)
                .loadDataFile(path + File.separator + filename)
                .build();
        List<String> ipSample = builtMMDBStatus.getIpv4Sample();
        ipSample.addAll(builtMMDBStatus.getIpv6Sample());

        for (String sampleIp : ipSample) {
            if (ObjectUtil.isEmpty(ipLookup.latLngLookup(sampleIp))) {
                JobUtil.infoLog("built ip locate library sample validate fail, sampleIp {}, latLng {}", sampleIp, ipLookup.latLngLookup(sampleIp));
                return false;
            }
        }
        return true;
    }
}
