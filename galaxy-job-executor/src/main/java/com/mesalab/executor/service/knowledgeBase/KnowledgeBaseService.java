package com.mesalab.executor.service.knowledgeBase;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.file.FileWriter;
import cn.hutool.core.util.CharsetUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.nacos.api.config.annotation.NacosValue;
import com.geedgenetworks.utils.StringUtil;
import com.mesalab.executor.core.utils.DBUtils;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.core.utils.JobUtil;
import com.mesalab.executor.core.utils.ZookeeperUtils;
import com.mesalab.executor.exception.BusinessException;
import com.mesalab.executor.pojo.JDBCParam;
import com.mesalab.executor.pojo.KnowledgeBase;
import com.mesalab.executor.pojo.MMDBStatus;
import org.springframework.stereotype.Service;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

import static com.mesalab.executor.core.utils.Constant.*;

@Service
public class KnowledgeBaseService {


    @NacosValue(value = "${zookeeper.server}", autoRefreshed = true)
    private String zookeeperServer;


    public boolean commonPushFile(Map sourceParams, Map<String, Object> sinkParam) throws Exception {
        String url = String.valueOf(sinkParam.get("url"));
        String params = String.valueOf(sinkParam.get("params"));
        JSONObject jsonObject = JSONUtil.parseObj(params);
        KnowledgeBase knowledgeBase = JSONUtil.toBean(jsonObject, KnowledgeBase.class);
        JDBCParam jdbcParam = new JDBCParam(sourceParams);
        int batch = Optional.ofNullable(jdbcParam.getBatch()).orElse(DB_BATCH);
        int pageNum = 0;
        //1. 数据转为csv 临时文件 2. 判断是否有变更，csv存入上传知识库
        String tmpDir = StrUtil.join(File.separator, WORK_PATH, "tmp", UUID.randomUUID());
        File tmpFile = new File(StringUtil.join(tmpDir, File.separator, knowledgeBase.getName(), ".", knowledgeBase.getFormat()));
        String sql = String.valueOf(sourceParams.get("sql"));

        boolean pushStatus = false;
        int size = 0;
        try {
            while (true) {
                int offset = pageNum * batch;
                String limitSql = StringUtil.join(sql, " LIMIT ", offset, ",", batch);
                sourceParams.put("sql", limitSql);
                //1.分页查库，2.追加创建文件
                List<Map> batchData = mysqlSource(sourceParams);
                size = batchData.size();
                creatFile(batchData, tmpFile.getAbsolutePath(), knowledgeBase.getFormat());
                if (size < batch) {
                    JobUtil.infoLog("knowledge file built over.");
                    break;
                }
                batchData.clear();
                pageNum++;
            }
            JobUtil.infoLog("knowledgeBase file info filename: {},file format: {} , size: {}", tmpFile.getName(), knowledgeBase.getFormat(), tmpFile.length());
            if (tmpFile.length() > 0) {
                pushStatus = push(knowledgeBase, tmpFile, url);
            }
        } catch (Exception e) {
            JobUtil.errorLog(e);
            throw new BusinessException(e);
        } finally {
            FileUtil.del(tmpDir);
        }
        return pushStatus;

    }

    public List<Map> mysqlSource(Map<String, String> sourceParams) throws Exception {
        JobUtil.infoLog("mysql data source starting, param is {}", sourceParams);
        JDBCParam jdbcParam = new JDBCParam(sourceParams);
        List<Map> collect = new ArrayList<>();
        try (Connection conn = DBUtils.getConnPool(jdbcParam);
             PreparedStatement pst = conn.prepareStatement(jdbcParam.getSql());
             ResultSet resultSet = pst.executeQuery()) {

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnLength = metaData.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> map = new HashMap<>();
                for (int i = 0; i < columnLength; i++) {
                    String columnName = metaData.getColumnName(i + 1);
                    Object value = resultSet.getObject(columnName);
                    map.put(columnName, value);
                }
                collect.add(map);
            }
        }
        return collect;
    }

    public File creatFile(List<Map> sourceData, String filePath, String format) throws Exception {
        File file = null;
        if ("json".equals(format)) {
            FileWriter fileWriter = new FileWriter(filePath, CharsetUtil.CHARSET_UTF_8);
            file = fileWriter.write(JSON.toJSONString(sourceData, SerializerFeature.PrettyFormat), true);
        } else if ("csv".equals(format)) {
            file = JobUtil.createCsvFile(sourceData, filePath, true);
        }
        return file;
    }

    /**
     * 上传文件到知识库
     *
     * @throws Exception
     */
    public boolean push(KnowledgeBase knowledgeBase, File tmpFile, String url) throws BusinessException {
        try {
            String resultStr = HttpClientUtils.httpPostFile(url, tmpFile, BeanUtil.beanToMap(knowledgeBase));
            JobUtil.infoLog("knowledgeBase name {} pushed, result {}", knowledgeBase.getName(), resultStr);
            if (!"-1".equals(resultStr)) {
                Map<String, Object> result = JSONUtil.toBean(resultStr, Map.class);
                if (result.get("status") != null && "200".equals(String.valueOf(result.get("status")))) {
                    JobUtil.infoLog("knowledgeBase push success");
                    return true;
                }
            } else {
                JobUtil.errorLog("knowledgeBase push fail");
            }
        } catch (Exception e) {
            JobUtil.errorLog(e);
            throw new BusinessException(e);
        }
        return false;
    }

    /**
     * 区分asn，ip 包含v4 v6
     */
    public boolean mmdbBuildPush(Map<String, Object> sinkParam) throws BusinessException {

        String url = String.valueOf(sinkParam.get("url"));
        String params = String.valueOf(sinkParam.get("params"));
        JSONObject jsonObject = JSONUtil.parseObj(params);
        KnowledgeBase knowledgeBase = JSONUtil.toBean(jsonObject, KnowledgeBase.class);
        boolean pushStatus = false;
        try {
            String script = "ip";
            Boolean ipLocation = true;

            if (knowledgeBase.getName().contains("asn")) {
                script = "asn";
                ipLocation = false;
            }
            int executeResult = buildMMDB(knowledgeBase, script);
            String filename = StringUtil.join(script, "_builtin.", knowledgeBase.getFormat());

            if (executeResult == 0) {
                if (ipLocation && !checkSample(KNOWLEDGE_WORK_DIR, filename, script)) {
                    JobUtil.infoLog("ip locate knowledgeBase perl execute or sample validate fail !");
                    return false;
                }
                //push knowledge
                File file = new File(StringUtil.join(KNOWLEDGE_WORK_DIR, File.separator, filename));
                JobUtil.infoLog("knowledgeBase file info filename: {},file format: {} , size: {}", file.getName(), knowledgeBase.getFormat(), file.length());
                if (file.length() > 0) {
                    pushStatus = push(knowledgeBase, file, url);
                }

            }
        } catch (Exception e) {
            JobUtil.errorLog(e);
            throw new BusinessException(e);
        }
        return pushStatus;
    }

    private int buildMMDB(KnowledgeBase knowledgeBase, String script) throws Exception {
        final String workDirCmd = "docker inspect --format '{{range.Mounts}}  {{.Source}} {{end}}' galaxy-job-executor |xargs -n 1|grep schedule-service";
        final String imagesNameCmd = "docker images --format '{{.Repository}}:{{.Tag}}' | grep '^perl:' | sort -r | head -n 1";

        String workDir = JobUtil.exeCmd(workDirCmd);
        String imagesName = JobUtil.exeCmd(imagesNameCmd);

        final String cmd = "docker run --rm --network=host -v " + workDir + "/mmdb:/mmdb --name perl-mmdb "+ imagesName +" sh -c 'cd /mmdb && perl " + script + ".pl " + knowledgeBase.getKb_id() + "'";
        JobUtil.infoLog("ip locate run perl cmd {}", cmd);

        int executeResult = JobUtil.executeCommand(cmd);
        JobUtil.infoLog("ip locate knowledgeBase perl execute result {}", executeResult == 0 ? "success" : "fail");
        return executeResult;
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

    public List<String> getMetadataIds() {
        try {
            ZookeeperUtils zk = new ZookeeperUtils();
            List<String> children = zk.getChildren(StrUtil.removeSuffix(ZK_KNOWLEDGE_PATH, "/"), zookeeperServer);
            return children;
        } catch (Exception e) {
            JobUtil.errorLog("knowledgeBase query zookeeper error :{}", e);
            return null;
        }
    }

    public boolean deleteMetadata(String knowledgeId) {
        try {
            ZookeeperUtils zk = new ZookeeperUtils();
            return zk.deleteNode(ZK_KNOWLEDGE_PATH + knowledgeId, zookeeperServer);
        } catch (Exception e) {
            JobUtil.errorLog("knowledgeBase query zookeeper error :{}", e);
            return false;
        }
    }

    public void modifyMetadata(String knowledgeId, String data) {
        ZookeeperUtils zk = new ZookeeperUtils();
        //是否存在 不存在创建
        zk.modifyNode(ZK_KNOWLEDGE_PATH + knowledgeId, data, zookeeperServer);
    }

    /**
     * 检查脚本运行结果和抽样ip是否打到包里
     *
     * @param
     * @return
     */
    public static boolean checkSample(String path, String filename, String script) throws Exception {

        final String builtinStatusFile = script+"_builtin_status.json";

        MMDBStatus mmdbStatus = JobUtil.readJsonFile(path + builtinStatusFile, MMDBStatus.class);
        JobUtil.infoLog("builtin ip locate library result: status {}, message {}", mmdbStatus.getStatus(), mmdbStatus.getMessage());
        if (200 != mmdbStatus.getStatus()) {
            JobUtil.errorLog("builtin status fail {}", mmdbStatus.getStatus());
            return false;
        }
        if (mmdbStatus.getTotal() == 0 ) {
            JobUtil.errorLog("perl query failed result: total {}.", mmdbStatus.getTotal());
            return false;
        }
        //测试抽样数据能查到结果说明打包成功
        List<String> ipSample = mmdbStatus.getIpSample();
        String samplePath = path + File.separator + filename;
        if ("ip".equals(script)) {
           return JobUtil.ipKnowledgeNotEmpty(samplePath,"",ipSample);
        }else if ("asn".equals(script)) {
           return JobUtil.asnKnowledgeNotEmpty(samplePath,"",ipSample);
        }
        return true;
    }

    public boolean deleteKnowledgeBase(List<Map> deleteKnowledgeBase, Map<String, Object> sinkParam) {
        try {
            List<String> metadataIds = getMetadataIds();

            int failCount = 0;
            for (Map deleteKnowledge : deleteKnowledgeBase) {
                String knowledgeId = String.valueOf(deleteKnowledge.get("knowledge_id"));
                if (metadataIds.contains(knowledgeId)) {
                    String resultStr = HttpClientUtils.httpDelete(String.valueOf(sinkParam.get("url")) + "/" + knowledgeId, null);
                    JobUtil.infoLog("knowledgeBase id {} delete, result {}", knowledgeId, resultStr);
                    if (!"-1".equals(resultStr)) {
                        Map<String, Object> result = JSONUtil.toBean(resultStr, Map.class);
                        if (result.get("status") != null && "200".equals(String.valueOf(result.get("status")))) {
                            deleteMetadata(knowledgeId);
                            JobUtil.infoLog("knowledgeBase delete success");
                        }else {
                            failCount ++;
                        }
                    } else {
                        JobUtil.errorLog("knowledgeBase delete fail");
                        failCount ++;
                    }
                }
            }
            if(failCount > 0 ){
                return false;
            }
            return true;
        } catch (Exception e) {
            JobUtil.errorLog(e);
            throw new BusinessException(e);
        }
    }
}
