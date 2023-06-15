package com.mesalab.executor.service.knowledgeBase;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.file.FileWriter;
import cn.hutool.core.util.CharsetUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.nacos.api.config.annotation.NacosValue;
import com.mesalab.executor.core.utils.DBUtils;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.core.utils.JobUtil;
import com.mesalab.executor.core.utils.ZookeeperUtils;
import com.mesalab.executor.exception.BusinessException;
import com.mesalab.executor.pojo.JDBCParam;
import com.mesalab.executor.pojo.KnowledgeBase;
import com.mesalab.executor.pojo.MMDBStatus;
import com.zdjizhi.utils.IpLookupV2;
import com.zdjizhi.utils.StringUtil;
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
                batchData = null;
                pageNum++;
            }
            pushStatus = push(knowledgeBase, tmpFile, url);
        } finally {
            FileUtil.del(tmpDir);
        }
        return pushStatus;

    }

    public List<Map> mysqlSource(Map<String, String> sourceParams) throws Exception {
        JobUtil.infoLog("mysql data source starting, param is {}", sourceParams);
        JDBCParam jdbcParam = new JDBCParam(sourceParams);
        List<Map> collect = new ArrayList<>();
        try (Connection conn = DBUtils.getDBConn(jdbcParam);
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

    public File creatFile(List<Map> sourceData, String filePath, String format) {
        File file = null;
        if ("json".equals(format)) {
            FileWriter fileWriter = new FileWriter(filePath, CharsetUtil.CHARSET_UTF_8);
            file = fileWriter.write(JSON.toJSONString(sourceData, SerializerFeature.PrettyFormat), true);
        } else if ("csv".equals(format)) {
            file = JobUtil.createCsvFile(sourceData, filePath, true);
        }
        sourceData.clear();
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
                    JobUtil.infoLog("ip locate knowledgeBase push success");
                    return true;
                }
            } else {
                JobUtil.errorLog("ip locate knowledgeBase push fail");
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
        final String workDirCmd = "docker inspect --format '{{range.Mounts}}  {{.Source}} {{end}}' galaxy-job-executor |xargs -n 1|grep plugins";
        String url = String.valueOf(sinkParam.get("url"));
        String params = String.valueOf(sinkParam.get("params"));
        JSONObject jsonObject = JSONUtil.parseObj(params);
        KnowledgeBase knowledgeBase = JSONUtil.toBean(jsonObject, KnowledgeBase.class);

        try {
            String workDir = JobUtil.exeCmd(workDirCmd);
            String script = "ip";
            JobUtil.infoLog("get docker work dir {}", workDir);
            Integer addrType = knowledgeBase.getType().contains("v6") ? 6 : 4;
            Boolean ipLocation = true;

            if (knowledgeBase.getType().contains("asn")) {
                script = "asn";
                ipLocation = false;
            }
            String cmd = "docker run --rm -v " + workDir + "/mmdb:/mmdb --name perl-mmdb perl:v3.1 sh -c 'cd /mmdb && perl " + script + ".pl " + addrType + " " + knowledgeBase.getId() + "'";
            JobUtil.infoLog("ip locate run docker perl cmd {}", cmd);

            int executeResult = JobUtil.executeCommand(cmd);
            JobUtil.infoLog("ip locate knowledgeBase perl execute result {}", executeResult == 0 ? "success" : "fail");
            String filename = StringUtil.join(script, addrType, ".", knowledgeBase.getFormat());

            if (executeResult == 0) {
                if (ipLocation && !checkIpMMDBSample(KNOWLEDGE_WORK_DIR, filename, script)) {
                    JobUtil.infoLog("ip locate knowledgeBase perl execute or sample validate fail !");
                    return false;
                }
                //push knowledge
                File file = new File(StringUtil.join(KNOWLEDGE_WORK_DIR, File.separator, filename));

                String resultStr = HttpClientUtils.httpPostFile(url, file, BeanUtil.beanToMap(knowledgeBase));
                JobUtil.infoLog("ip  knowledgeBase push knowledge result: {},\n message {}", knowledgeBase, resultStr);
                if ("-1".equals(resultStr)) {
                    JobUtil.errorLog("ip locate knowledgeBase push fail !");
                    return false;
                }
                Map<String, Object> result = JSONUtil.toBean(resultStr, Map.class);
                if (result.get("status") != null && "200".equals(String.valueOf(result.get("status")))) {
                    JobUtil.infoLog("ip locate knowledgeBase push success !");
                    return true;
                }
            }
        } catch (Exception e) {
            JobUtil.errorLog(e);
            throw new BusinessException(e);
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
     * 检查脚本运行结果和抽样ip是否打到包里
     *
     * @param
     * @return
     */
    public static boolean checkIpMMDBSample(String path, String filename, String script) throws Exception {

        String builtStatusFile = "built-status.json";

        MMDBStatus builtMMDBStatus = JobUtil.readJsonFile(path + builtStatusFile, MMDBStatus.class);
        JobUtil.infoLog("built ip locate library result: status {}, message {}", builtMMDBStatus.getStatus(), builtMMDBStatus.getMessage());
        if (200 != builtMMDBStatus.getStatus()) {
            JobUtil.errorLog("built status fail {}", builtMMDBStatus.getStatus());
            return false;
        }
        //测试抽样数据能查到结果说明打包成功
        IpLookupV2.Builder builder = new IpLookupV2.Builder(false);
        List<String> ipSample = builtMMDBStatus.getIpv4Sample();
        ipSample.addAll(builtMMDBStatus.getIpv6Sample());
        for (String sampleIp : ipSample) {
            if ("ip".equals(script)) {
                IpLookupV2 ipLookup = builder.loadDataFile(path + File.separator + filename).build();
                if (ObjectUtil.isEmpty(ipLookup.latLngLookup(sampleIp))) {
                    JobUtil.infoLog("built ip locate library sample validate fail, sampleIp {}, latLng {}", sampleIp, ipLookup.latLngLookup(sampleIp));
                    return false;
                }
            } else if ("asn".equals(script)) {
                IpLookupV2 ipLookup = builder.loadAsnDataFile(path + File.separator + filename).build();
                if (ObjectUtil.isEmpty(ipLookup.asnLookup(sampleIp))) {
                    JobUtil.infoLog("built asn library sample validate fail, sampleIp {}, asn {}", sampleIp, ipLookup.asnLookup(sampleIp));
                    return false;
                }
            }
        }
        return true;
    }
}
