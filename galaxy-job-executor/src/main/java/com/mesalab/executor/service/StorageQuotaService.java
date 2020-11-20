package com.mesalab.executor.service;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.mesalab.executor.core.config.StorgeConfig;
import com.mesalab.executor.core.utils.Constant;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.core.utils.ZookeeperUtils;
import com.mesalab.executor.exception.BusinessException;
import com.xxl.job.core.log.XxlJobLogger;
import com.zdjizhi.utils.DateUtils;
import com.zdjizhi.utils.JsonMapper;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 存储配额获取指标类 组合 原始日志结果对象,统计日志结果对象,文件系统结果对象
 *
 * @author qidaijie
 */
@Service
public class StorageQuotaService {

    private static Logger logger = LoggerFactory.getLogger(StorageQuotaService.class);
    /**
     * 调用Linux命令返回日志编码
     */
    private static final String CHART_SET_UTF8 = "UTF-8";
    /**
     * HBase 单机数据目录在linux上的位置
     */
    private static String hbaselinuxDir;
    /**
     * HBase 在HDFS上的路径
     */
    private static String hbaseHdfsDir;
    /**
     * Druid rest地址
     */
    private static String queryDruidUrl;
    /**
     * ClickHouse rest地址
     */
    private static String queryClickHouseUrl;

    private static String database;
    /**
     * HBase 所在HDFS集群地址
     */
    private static String queryHdfsUrls;

    private static String trafficUserName;
    private static String trafficSecret;

    private static String systemPartsCluster;
    private static String systemDisks;
    private static boolean isCluster;
    private static final String ZOOKEEPER_STORAGE_PATH = "/storage/worker/";

    private static final String[] dangerCharacters = {"<", ">", "\"", "\'", "%", "(", ")", "\\", "."};

    @Value("${zookeeper.server}")
    private String zookeeperServer;

    @Autowired
    public StorageQuotaService(StorgeConfig config) {
        database = config.getTrafficDatasource();
        trafficUserName = config.getTrafficUsername();
        trafficSecret = config.getTrafficSecret();
        queryClickHouseUrl = Joiner.on("").join(Constant.SERVER_PROTOCOL, config.getTrafficServer());
        systemPartsCluster = config.getSystemPartsCluster();
        systemDisks = config.getSystemDisks();

        queryDruidUrl = Joiner.on("").join(Constant.SERVER_PROTOCOL, config.getAnalyticServer(), "/druid/v2/sql");
        queryHdfsUrls = config.getFilesHdfsServer();
        hbaselinuxDir = config.getHbaseLinuxDir();
        hbaseHdfsDir = config.getHbaseHdfsDir();
        isCluster = config.getIsClusder();
    }

    public static String filterParameter(String parameter) {
        if (StringUtils.isEmpty(parameter)) return null;
        StringBuffer sb = new StringBuffer(parameter);
        for (String s : dangerCharacters) {
            while (sb.indexOf(s) > -1) {
                sb.deleteCharAt(sb.indexOf(s));
            }
        }
        return sb.toString();
    }

    /**
     * 用于获取 ClickHouse 当前存储大小 若获取失败查询druid内最新的值替补当前值
     */
    private StorageQuota getClickHouseCurr() {
        Map<String, Object> deleteParamMap = getDeleteSource();
        StorageQuota storageQuota = new StorageQuota();
        String currSql = "SELECT SUM(`bytes_on_disk`) FROM " + systemPartsCluster + " WHERE database = '" + database + "';";
        try {
            deleteParamMap.put("query", currSql);
            String currResult = HttpClientUtils
                    .httpGet(HttpClientUtils.getUrlWithParams(queryClickHouseUrl, deleteParamMap));
            Long result = Long.valueOf(currResult.trim());

            storageQuota.setData(ImmutableMap.of("used_size", result));
            logger.info("查询 clickhouse used_size 成功, {}", result);
            return storageQuota;
        } catch (Exception e) {
            Long trafficLogs = getCurrBefore(Constant.TRAFFIC_LOGS);
            storageQuota.setData(ImmutableMap.of("used_size", trafficLogs));
            storageQuota.setStatus(ImmutableMap.of("traffic_current", "fail " ));
            logger.error("查询 clickhouse used_size 失败, {}",e);
            XxlJobLogger.log("查询 clickhouse used_size 失败, {}",e);
            return storageQuota;
        }

    }

    /**
     * 用于获取ClickHouse最大存储大小 若获取失败查询druid内最新的值替补当前值
     */
    private StorageQuota getClickHouseMax() {
        Map<String, Object> deleteParamMap = getDeleteSource();
        StorageQuota storageQuota = new StorageQuota();
        String maxSql = "SELECT SUM(`total_space`) FROM " + systemDisks + ";";
        try {
            deleteParamMap.put("query", maxSql);
            String maxResult = HttpClientUtils
                    .httpGet(HttpClientUtils.getUrlWithParams(queryClickHouseUrl, deleteParamMap));
            long result = Long.parseLong(maxResult.trim());
            storageQuota.setData(ImmutableMap.of("max_size", result));
            logger.info("查询 clickhouse max_size 成功 ,{}", result);
            return storageQuota;
        } catch (Exception e) {
            Long trafficLogs = getMaxBefore(Constant.TRAFFIC_LOGS);
            storageQuota.setData(ImmutableMap.of("max_size", trafficLogs));
            storageQuota.setStatus(ImmutableMap.of("traffic_max", "fail " ));
            logger.error("查询 clickhouse max_size 失败,{}",e);
            XxlJobLogger.log("查询 clickhouse max_size 失败,{}",e);
            return storageQuota;
        }
    }

    /**
     * 用于获取ClickHouse 差值 若获取失败直接写入0
     */
    private StorageQuota getClickHouseDiff() {
        String date = DateUtils.getDateOfYesterday("yyyyMMdd");
        Map<String, Object> deleteParamMap = getDeleteSource();
        StorageQuota storageQuota = new StorageQuota();
        String diffSql = "SELECT SUM(bytes_on_disk) FROM " + systemPartsCluster + " WHERE database = '" + database
                + "' AND partition = '" + date + "';";
        try {
            deleteParamMap.put("query", diffSql);
            String diffResult = HttpClientUtils
                    .httpGet(HttpClientUtils.getUrlWithParams(queryClickHouseUrl, deleteParamMap));
            long result = Long.parseLong(diffResult.trim());
            storageQuota.setData(ImmutableMap.of("aggregate_size", result));
            logger.info("查询 clickhouse traffic_aggregate 成功,{}", result);
            return storageQuota;
        } catch (Exception e) {
            storageQuota.setData(ImmutableMap.of("aggregate_size", 0L));
            storageQuota.setStatus(ImmutableMap.of("traffic_aggregate", "fail " ));
            logger.error("查询clickhouse traffic_aggregate 失败,{}",e);
            XxlJobLogger.log("查询clickhouse traffic_aggregate 失败,{}",e);
            return storageQuota;
        }
    }

//========================Druid==============================

    /**
     * 获取Druid当前存储大小 若获取失败直接补0，Druid本身无法提供服务无需再查询上次值
     */
    private StorageQuota getDruidCurr() {
        String currSql = "{\"query\":\"SELECT SUM(curr_size) AS curr_size FROM sys.servers WHERE server_type = 'historical'\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";
        StorageQuota storageQuota = new StorageQuota();
        try {
            String currResult = HttpClientUtils.httpPost(queryDruidUrl, currSql);
            List<Map<String, Object>> list = (List) JsonMapper.fromJsonString(currResult, List.class);
            Long currSize = Long.valueOf(String.valueOf(list.get(0).get("curr_size")));

            storageQuota.setData(ImmutableMap.of("used_size", currSize));
            logger.info("查询 druid used_size 成功,{}", currSize);
            return storageQuota;
        } catch (Exception e) {
            storageQuota.setData(ImmutableMap.of("used_size", 0L));
            storageQuota.setStatus(ImmutableMap.of("analytic_current", "fail " ));
            logger.error("查询 druid used_size 失败,{}",e);
            XxlJobLogger.log("查询 druid used_size 失败,{}",e);
            return storageQuota;
        }
    }

    /**
     * 获取Druid过去30秒增量 若获取失败直接补0，Druid本身无法提供服务无需再查询上次值
     */
    private StorageQuota getDruidDiff(Long currSize) {
        StorageQuota storageQuota = new StorageQuota();
        try {
            Long before = getCurrBefore(Constant.REPORT_AND_METRICS);
            long diff = getDiffNum(currSize, before);
            storageQuota.setData(ImmutableMap.of("aggregate_size", diff));
            logger.info("查询 druid aggregate_size 成功,{}", diff);
            return storageQuota;
        } catch (Exception e) {
            storageQuota.setData(ImmutableMap.of("aggregate_size", 0L));
            storageQuota.setStatus(ImmutableMap.of("analytic_aggregate", "fail " ));
            logger.error("查询druid aggregate_size 失败,{}",e);
            XxlJobLogger.log("查询druid aggregate_size 失败,{}",e);
            return storageQuota;
        }
    }

    /**
     * 获取Druid最大存储大小 若获取失败直接补0，Druid本身无法提供服务无需再查询上次值
     */
    private StorageQuota getDruidMax() {
        String maxSql = "{\"query\":\"SELECT SUM(max_size) AS max_size FROM sys.servers WHERE server_type = 'historical'\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";
        StorageQuota storageQuota = new StorageQuota();
        try {
            String maxResult = HttpClientUtils.httpPost(queryDruidUrl, maxSql);
            List<Map<String, Long>> list = (List) JsonMapper.fromJsonString(maxResult, List.class);
            Long maxSize = list.get(0).get("max_size");
            storageQuota.setData(ImmutableMap.of("max_size", maxSize));
            logger.info("查询 druid max_size 成功，{}", maxSize);
            return storageQuota;
        } catch (Exception e) {
            storageQuota.setData(ImmutableMap.of("max_size", 0L));
            storageQuota.setStatus(ImmutableMap.of("analytic_max", "fail " ));
            logger.error("查询 druid max_size 失败，{}",e);
            XxlJobLogger.log("查询 druid max_size 失败，{}",e);
            return storageQuota;
        }
    }

//======================================================

    /**
     * 获取HBase 单机当前存储大小 若获取失败查询druid内最新的值替补当前值
     */
    /*private StorageQuota getHBaseAloneCurr() {
        StorageQuota storageQuota = new StorageQuota();
        try {
            Process catCommands = Runtime.getRuntime()
                    .exec(new String[]{"/bin/sh", "-c", "du -s " + hbaselinuxDir + " | awk '{print $1}'"});
            String message = getProcessMessage(catCommands.getInputStream(), CHART_SET_UTF8);
            logger.info("查询 hbase alone used_size 命令行结果，{}", message);
            catCommands.waitFor();

            Long result = Long.parseLong(message.trim()) * 1024L;
            storageQuota.setData(ImmutableMap.of("used_size", result));
            logger.info("查询 hbase alone used_size 成功，{}", storageQuota);
            return storageQuota;
        } catch (Exception e) {
            Long trafficLogs = getCurrBefore(Constant.FILES);
            storageQuota.setData(ImmutableMap.of("used_size", trafficLogs));
            storageQuota.setStatus(ImmutableMap.of("files_current", "fail " ));
            logger.error("查询 hbase alone used_size 失败，{}",e);
            XxlJobLogger.log("查询 hbase alone used_size 失败，{}",e);
            return storageQuota;
        }
    }*/

    /**
     * 获取HBase 单机最大存储大小 若获取失败查询druid内最新的值替补当前值
     */
    /*private StorageQuota getHBaseAloneMax() {
        StorageQuota storageQuota = new StorageQuota();
        try {
            Process catCommands = Runtime.getRuntime().exec(
                    new String[]{"/bin/sh", "-c", "df " + hbaselinuxDir + " | sed -n '2,2p' | awk '{print $2}'"});
            String message = getProcessMessage(catCommands.getInputStream(), CHART_SET_UTF8);
            logger.info("查询 hbase alone max_size 命令行结果，{}", message);
            catCommands.waitFor();

            Long result = Long.parseLong(message.trim()) * 1024L;
            storageQuota.setData(ImmutableMap.of("max_size", result));
            logger.info("查询 hbase max_size alone  max_size 成功，{}", storageQuota);
            return storageQuota;
        } catch (Exception e) {
            Long trafficLogs = getMaxBefore(Constant.FILES);
            storageQuota.setData(ImmutableMap.of("max_size", trafficLogs));
            storageQuota.setStatus(ImmutableMap.of("files_max", "fail " ));
            logger.error("查询 hbase alone max_size 失败，{}",e);
            XxlJobLogger.log("查询 hbase alone max_size 失败，{}",e);
            return storageQuota;
        }
    }*/

    /**
     * 获取HBase 单机存储差值 若获取失败直接补0
     */
    /*private StorageQuota getHBaseAloneDiff() {
        StorageQuota storageQuota = new StorageQuota();
        try {
            Process catCommands = Runtime.getRuntime()
                    .exec(new String[]{"/bin/sh", "-c", "du -s " + hbaselinuxDir + " | awk '{print $1}'"});
            String message = getProcessMessage(catCommands.getInputStream(), CHART_SET_UTF8);
            logger.info("查询 hbase alone aggregate_size 命令行结果，{}", message);
            catCommands.waitFor();

            long result = Long.parseLong(message.trim()) * 1024L;
            Long trafficLogs = getCurrBefore(Constant.FILES);
            long diff = getDiffNum(result, trafficLogs);
            storageQuota.setData(ImmutableMap.of("aggregate_size", diff));
            logger.info("查询 hbase alone aggregate_size 成功，{}", storageQuota);
            return storageQuota;
        } catch (Exception e) {
            storageQuota.setData(ImmutableMap.of("aggregate_size", 0L));
            storageQuota.setStatus(ImmutableMap.of("files_aggregate", "fail " ));
            logger.error("查询 hbase alone aggregate_size 失败，{}",e);
            XxlJobLogger.log("查询 hbase alone aggregate_size 失败，{}",e);
            return storageQuota;
        }
    }*/

//=========================HBase集群模式=============================

    /**
     * 获取HBase 集群当前存储大小 若获取失败查询druid内最新的值替补当前值
     */
    private StorageQuota getHBaseClusterCurr(String queryHdfsUrl) {
        StorageQuota storageQuota = new StorageQuota();
        try {
            String currDir = queryHdfsUrl + "/webhdfs/v1" + hbaseHdfsDir + "?op=GETCONTENTSUMMARY";
            String diffResult = HttpClientUtils.httpGet(currDir);
            Map map = (Map) JsonMapper.fromJsonString(diffResult, Map.class);
            Map contentSummary = (Map) map.get("ContentSummary");
            Long length = Long.valueOf(String.valueOf(contentSummary.get("length")));

            storageQuota.setData(ImmutableMap.of("used_size", length));
            logger.info("查询 hbase cluster used_size 成功，{}", length);
            return storageQuota;
        } catch (Exception e) {
            Long trafficLogs = getCurrBefore(Constant.FILES);
            storageQuota.setData(ImmutableMap.of("used_size", trafficLogs));
            storageQuota.setStatus(ImmutableMap.of("files_current", "fail " ));
            logger.error("查询 hbase cluster used_size 失败，{}",e);
            XxlJobLogger.log("查询 hbase cluster used_size 失败，{}",e);
            return storageQuota;
        }
    }

    /**
     * 获取HBase 集群最大存储大小 若获取失败查询druid内最新的值替补当前值
     */
    private StorageQuota getHBaseClusterMax(String queryHdfsUrl) {
        StorageQuota storageQuota = new StorageQuota();
        try {
            String currDir = queryHdfsUrl + "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo";
            String diffResult = HttpClientUtils.httpGet(currDir);
            Map map = (Map) JsonMapper.fromJsonString(diffResult, Map.class);
            List<Map> beans = (List<Map>) map.get("beans");
            Long total = Long.valueOf(String.valueOf(beans.get(0).get("Total")));

            storageQuota.setData(ImmutableMap.of("max_size", total));
            logger.info("查询 hbase cluster max_size 成功,{}", total);
            return storageQuota;
        } catch (Exception e) {
            Long trafficLogs = getMaxBefore(Constant.FILES);

            storageQuota.setData(ImmutableMap.of("max_size", trafficLogs));
            storageQuota.setStatus(ImmutableMap.of("files_max", "fail " ));
            logger.error("查询 hbase cluster max_size 失败，{}",e);
            XxlJobLogger.log("查询 hbase cluster max_size 失败，{}",e);
            return storageQuota;
        }
    }

    /**
     * 获取HBase 集群当前存储大小 若获取失败直接补0
     */
    private StorageQuota getHBaseClusterDiff(String queryHdfsUrl) {
        StorageQuota storageQuota = new StorageQuota();
        try {
            String currDir = queryHdfsUrl + "/webhdfs/v1" + hbaseHdfsDir + "?op=GETCONTENTSUMMARY ";
            String diffResult = HttpClientUtils.httpGet(currDir);
            Map map = (Map) JsonMapper.fromJsonString(diffResult, Map.class);
            Map contentSummary = (Map) map.get("ContentSummary");
            Long length = Long.valueOf(String.valueOf(contentSummary.get("length")));

            Long trafficLogs = getCurrBefore(Constant.FILES);
            long diff = getDiffNum(length, trafficLogs);
            storageQuota.setData(ImmutableMap.of("aggregate_size", diff));
            logger.info("查询 HBase cluster aggregate_size 成功,{}", diff);
            return storageQuota;
        } catch (Exception e) {
            storageQuota.setData(ImmutableMap.of("aggregate_size", 0L));
            storageQuota.setStatus(ImmutableMap.of("files_aggregate", "fail " ));
            logger.error("查询 hbase cluster aggregate_size 失败，{}",e);
            XxlJobLogger.log("查询 hbase cluster aggregate_size 失败，{}",e);
            return storageQuota;
        }
    }

//=======================工具类方法===============================

    /**
     * 用于通过druid获取上次对应类型的Curr值
     *
     * @param logType 统计类型
     * @return 上次的值
     */
    private Long getCurrBefore(String logType) {
        try {
            String currSql = "{\"query\":\"SELECT used_size FROM sys_storage_log WHERE log_type = '" + logType
                    + "' ORDER BY __time DESC LIMIT 1\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";
            String currResult = HttpClientUtils.httpPost(queryDruidUrl, currSql);
            List<Map> list = (List) JsonMapper.fromJsonString(currResult, List.class);
            Long currSize = Long.valueOf(String.valueOf(list.get(0).get("used_size")));
            logger.info("查询 druid 上次 used_size 成功，{}", currSize);
            return currSize;
        } catch (Exception e) {
            logger.error("查询druid上次 used_size 失败，{}",e);
            XxlJobLogger.log("查询druid上次 used_size 失败，{}",e);
            return 0L;
        }
    }

    /**
     * 用于通过druid获取上次对应类型的Max值
     *
     * @param logType 统计类型
     * @return 上次的值
     */
    private Long getMaxBefore(String logType) {
        try {
            String maxSql = "{\"query\":\"SELECT max_size FROM sys_storage_log WHERE log_type = '" + logType
                    + "' ORDER BY __time DESC LIMIT 1\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";
            String maxResult = HttpClientUtils.httpPost(queryDruidUrl, maxSql);
            List<Map> list = (List<Map>) JsonMapper.fromJsonString(maxResult, List.class);
            Long currSize = Long.valueOf(String.valueOf(list.get(0).get("max_size")));
            logger.info("查询 druid max_size 上次值成功,{}", currSize);
            return currSize;
        } catch (Exception e) {
            logger.error("查询 druid max_size 上次值失败，{}",e);
            XxlJobLogger.log("查询 druid max_size 上次值失败，{}",e);
            return 0L;
        }
    }

    /**
     * @param node
     * @return 自定义的标准时间 last_storage,
     * 存在zookeeper 节点/storage/worker/+node
     */
    public Long getLastStorage(String node) {
        try {
            ZookeeperUtils zk = new ZookeeperUtils();
            //查询
            String nodeData = zk.getNodeData(ZOOKEEPER_STORAGE_PATH + node, zookeeperServer);
            //不存在创建一个
            if (ObjectUtils.isEmpty(nodeData)) {
                Long lastTime = System.currentTimeMillis() / 1000;
                zk.modifyNode(ZOOKEEPER_STORAGE_PATH + node, String.valueOf(lastTime), zookeeperServer);
                return lastTime;
            }
            Long lastStorage = Long.valueOf(nodeData);
            logger.info("查询标准时间 last_storage成功,{}", lastStorage);

            return Long.valueOf(nodeData);
        } catch (Exception e) {
            logger.error("查询标准时间 last_storage 失败,{}",e);
            throw new BusinessException("查询标准时间 last_storage 失败 ," );
        }

    }

    /**
     * 修改zookeeper节点信息
     * <p>
     * path 节点路径/storage/worker/ +node
     */
    public void modifyLastStorage(String node, String data) {
        ZookeeperUtils zk = new ZookeeperUtils();
        //是否存在 不存在创建
        zk.modifyNode(ZOOKEEPER_STORAGE_PATH + node, data, zookeeperServer);
    }

    /**
     * 数据库连接参数
     *
     * @return map {database,password：,user:,}
     */
    private Map<String, Object> getDeleteSource() {
        // sql参数拼接
        Map<String, Object> deleteParamMap = Maps.newHashMap();
        deleteParamMap.put("password", trafficSecret);
        deleteParamMap.put("user", trafficUserName);
        return deleteParamMap;
    }

    /**
     * 解析脚本返回结果
     *
     * @param in
     * @param charset 编码格式
     */
    /*private String getProcessMessage(InputStream in, String charset) {
        if (in != null) {
            BufferedReader br = null;
            StringBuffer buffer = new StringBuffer();
            try {
                br = new BufferedReader(
                        new InputStreamReader(in, StringUtil.isBlank(charset) ? CHART_SET_UTF8 : charset));
                String line = null;
                while ((line = br.readLine()) != null) {
                    buffer.append(line);
                }
            } catch (IOException e) {
                logger.error("解析返回结果IO错误，{}",e);

            } finally {
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        logger.error("解析返回结果IO错误，{}",e);

                    }
                }
            }
            return buffer.toString();
        } else {
            return "";
        }
    }*/

    /**
     * 用于组合正常维度的JSON串
     *
     * @return 结果json
     */
    public Map<String, Map> getDiskJson() {

        Map all = new HashMap<>();
        List<Map> data = new ArrayList<>();
        Long now = System.currentTimeMillis() / 1000;
        Map<String, Integer> status = new HashMap<>();
        status.put("trafficFailCount", getCKStorageJson(data, now));
        status.put("reportFailCount", getDruidStorageJson(data, now));
        if(isCluster){
            status.put("fileFailCount", getHbaseStorageJson(data, now));
        } /*else {
            status.put("fileFailCount", getHbaseStorageJsonAlone(data, now));
        }*/

        all.put("data", data);
        all.put("status", status);

        return all;
    }

    private int getCKStorageJson(List<Map> data, Long time) {
        int ckErrorCount = 0;
        StorageQuota clickHouseCurr = getClickHouseCurr();
        StorageQuota clickHouseMax = getClickHouseMax();

        if (clickHouseCurr.getStatus() != null || clickHouseMax.getStatus() != null) {
            XxlJobLogger.log("query clickhouse storage error message clickHouseCurr ={} , clickHouseMax={}", clickHouseCurr.getStatus(), clickHouseMax.getStatus());
            ckErrorCount++;
        }

        Map<String,Object> traffic = new HashMap<>();
        traffic.put("log_type", Constant.TRAFFIC_LOGS);
        traffic.put("time", time);
        traffic.put("last_storage", getLastStorage(Constant.ZK_TRAFFIC_LOGS));
        traffic.putAll(clickHouseCurr.getData());
        traffic.putAll(clickHouseMax.getData());
        data.add(traffic);
        return ckErrorCount;
    }

    private int getDruidStorageJson(List<Map> data, Long time) {
        StorageQuota druidCurr = getDruidCurr();
        StorageQuota druidDiff = getDruidDiff(Long.valueOf(String.valueOf(druidCurr.getData().get("used_size"))));
        StorageQuota druidMax = getDruidMax();
        int druidErrorCount = 0;
        if (druidCurr.getStatus() != null || druidDiff.getStatus() != null || druidMax.getStatus() != null) {
            XxlJobLogger.log("query druid storage error message druidCurr ={} , druidDiff={}, druidMax={}", druidCurr.getStatus(), druidDiff.getStatus(), druidMax.getStatus());
            druidErrorCount++;
        }

        Map metrics = new HashMap<>();
        metrics.put("log_type", Constant.REPORT_AND_METRICS);
        metrics.put("time", time);
        metrics.put("last_storage", getLastStorage(Constant.ZK_REPORT_AND_METRICS));
        metrics.putAll(druidCurr.getData());
        metrics.putAll(druidDiff.getData());
        metrics.putAll(druidMax.getData());
        data.add(metrics);
        return druidErrorCount;
    }
    /*
     * 集群hbase存储配额
     **/
    private int getHbaseStorageJson(List<Map> data, Long time) {
        int hbaseErrorCount = 0;
        List<String> queryHdfsUrlList = Splitter.on(",").trimResults().omitEmptyStrings().splitToList(queryHdfsUrls);
        for (String queryHdfsUrl : queryHdfsUrlList) {
            try {
                queryHdfsUrl = Constant.SERVER_PROTOCOL.concat(queryHdfsUrl);
                StorageQuota hBaseCurr = getHBaseClusterCurr(queryHdfsUrl);
                StorageQuota hBaseMax = getHBaseClusterMax(queryHdfsUrl);
                StorageQuota hBaseDiff = getHBaseClusterDiff(queryHdfsUrl);

                if (hBaseCurr.getStatus() != null || hBaseDiff.getStatus() != null || hBaseMax.getStatus() != null) {
                    XxlJobLogger.log("查询 hbase storage error message, endpoint = {}, hBaseCurr ={} , hBaseDiff={}, hBaseMax={}", queryHdfsUrl, hBaseCurr.getStatus(), hBaseDiff.getStatus(), hBaseMax.getStatus());
                    hbaseErrorCount++;
                }
                Map files = new HashMap<>();
                files.put("log_type", Constant.FILES);
                files.put("time", time);
                files.put("last_storage", getLastStorage(Constant.FILES));
                files.putAll(hBaseCurr.getData());
                files.putAll(hBaseMax.getData());
                files.putAll(hBaseDiff.getData());
                data.add(files);
            } catch (Exception e) {
                logger.error("查询 hbase 存储配额发生错误：endpoint = {}, error = {}", queryHdfsUrl,e);
                XxlJobLogger.log("查询 hbase 存储配额发生错误：endpoint = {}, error = {}", queryHdfsUrl,e);
                hbaseErrorCount++;
            }
        }
        return hbaseErrorCount;
    }

    /**
     * 单机hbase
     * @param data
     * @param time
     * @return
     */
    /*private int getHbaseStorageJsonAlone(List<Map> data, Long time) {
        int hbaseErrorCount = 0;
        try {
            StorageQuota hBaseCurr = getHBaseAloneCurr();
            StorageQuota hBaseMax = getHBaseAloneMax();
            StorageQuota hBaseDiff = getHBaseAloneDiff();
            if (hBaseCurr.getStatus() != null || hBaseDiff.getStatus() != null || hBaseMax.getStatus() != null) {
                XxlJobLogger.log("query hbase storage error message, endpoint = {}, hBaseCurr ={} , hBaseDiff={}, hBaseMax={}", hBaseCurr.getStatus(), hBaseDiff.getStatus(), hBaseMax.getStatus());
                hbaseErrorCount++;
            }
            Map files = new HashMap<>();
            files.put("log_type", Constant.FILES);
            files.put("time", time);
            files.put("last_storage", getLastStorage(Constant.FILES));
            files.putAll(hBaseCurr.getData());
            files.putAll(hBaseMax.getData());
            files.putAll(hBaseDiff.getData());
            data.add(files);
        } catch (Exception e) {
            logger.error("查询单机hbase 存储配额发生错误：endpoint = {}, error = {}", e);
            XxlJobLogger.log("查询单机hbase 存储配额发生错误：endpoint = {}, error = {}", e);
            hbaseErrorCount++;
        }
        return hbaseErrorCount;
    }*/

    /**
     * 用于组合ClickHouse一天执行一次的JSON串 特殊处理
     *
     * @return 结果json
     */
    public Map getDayJson() {
        Map all = new HashMap<>();
        List<Map> data = new ArrayList<>();
        Long now = System.currentTimeMillis() / 1000;
        int ckFailCount = getCKStorageJson(data, now);

        StorageQuota clickHouseDiff = getClickHouseDiff();
        if (clickHouseDiff.getStatus() != null) {
            XxlJobLogger.log("query clickhouse storage error message clickHouseDiff ={} ", clickHouseDiff.getStatus());
            ckFailCount++;
        }
        data.get(0).putAll(clickHouseDiff.getData());

        Map<String, Integer> status = new HashMap<>();
        status.put("trafficFailCount", ckFailCount);
        all.put("status", status);
        all.put("data", data);
        return all;
    }

    /**
     * 获取差值计算，若为负数则填写0
     *
     * @param now    这一次的值
     * @param before 上一次的值
     * @return 差值
     */
    private Long getDiffNum(Long now, Long before) {
        long diff = now - before;
        if (diff >= 0) {
            return diff;
        } else {
            return 0L;
        }
    }

    @Data
    class StorageQuota {

        private Map<String, Object> data;

        private Map<String, String> status;
    }
}
