package com.mesalab.executor.service;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.mesalab.executor.core.config.StorgeConfig;
import com.mesalab.executor.core.utils.*;
import com.mesalab.executor.exception.BusinessException;
import com.mesalab.executor.pojo.HosSpace;
import com.xxl.job.core.log.XxlJobLogger;
import com.zdjizhi.utils.DateUtils;
import com.zdjizhi.utils.JsonMapper;
import lombok.Data;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import java.util.*;

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
     * Druid rest地址
     */
    private static String queryDruidUrl;
    /**
     * ClickHouse rest地址
     */
    private static String queryClickHouseUrl;

    private static String database;
    /**
     * hos地址
     */
    private static String filesHosServer;
    private static String filesToken;

    private static String trafficUserName;
    private static String trafficPassword;

    private static String systemPartsCluster;
    private static String systemDisks;
    private static final String ZOOKEEPER_STORAGE_PATH = "/storage/worker/";
    private static final String FILE_STORAGE_PATH = "/admin/diskspace";

    @Value("${zookeeper.server}")
    private String zookeeperServer;

    @Autowired
    public StorageQuotaService(StorgeConfig config) {
        database = config.getTrafficDatasource();
        trafficUserName = config.getTrafficUsername();
        trafficPassword = config.getTrafficPassword();
        queryClickHouseUrl = Joiner.on("").join(Constant.HTTP, config.getTrafficServer());
        systemPartsCluster = config.getSystemPartsCluster();
        systemDisks = config.getSystemDisks();

        queryDruidUrl = Joiner.on("").join(Constant.HTTP, config.getAnalyticServer(), "/druid/v2/sql");
        filesHosServer = config.getFilesHosServer();
        filesToken = config.getFilesToken();
    }

    /**
     * 用于获取 ClickHouse 当前存储大小 若获取 failquerydruid内最新的值替补当前值
     */
    private StorageQuota getClickHouseCurr() {
        Map<String, Object> deleteParamMap = getDeleteSource();
        StorageQuota storageQuota = new StorageQuota();
        String currSql = "SELECT SUM(`bytes_on_disk`) FROM " + systemPartsCluster + " WHERE database = '" + database + "';";
        deleteParamMap.put("query", currSql);
        String currResult = HttpClientUtils.httpGet(HttpClientUtils.getUrlWithParams(queryClickHouseUrl, deleteParamMap));
        Long result = Long.valueOf(currResult.trim());

        storageQuota.setData(ImmutableMap.of("used_size", result));
        logger.info("query clickhouse used_size success, {}", storageQuota);
        return storageQuota;

    }

    /**
     * 用于获取ClickHouse最大存储大小 若获取 failquerydruid内最新的值替补当前值
     */
    private StorageQuota getClickHouseMax() {
        Map<String, Object> deleteParamMap = getDeleteSource();
        StorageQuota storageQuota = new StorageQuota();
        String maxSql = "SELECT SUM(`total_space`) FROM " + systemDisks + ";";
        deleteParamMap.put("query", maxSql);
        String maxResult = HttpClientUtils
                .httpGet(HttpClientUtils.getUrlWithParams(queryClickHouseUrl, deleteParamMap));
        long result = Long.parseLong(maxResult.trim());
        storageQuota.setData(ImmutableMap.of("max_size", result));
        logger.info("query clickhouse max_size success ,{}", storageQuota);
        return storageQuota;
    }

    /**
     * 用于获取ClickHouse 差值 若获取 fail直接写入0
     */
    private StorageQuota getClickHouseDiff() {
        String date = DateUtils.getDateOfYesterday("yyyyMMdd");
        Map<String, Object> deleteParamMap = getDeleteSource();
        StorageQuota storageQuota = new StorageQuota();
        String diffSql = "SELECT SUM(bytes_on_disk) FROM " + systemPartsCluster + " WHERE database = '" + database
                + "' AND partition = '" + date + "';";
        deleteParamMap.put("query", diffSql);
        String diffResult = HttpClientUtils
                .httpGet(HttpClientUtils.getUrlWithParams(queryClickHouseUrl, deleteParamMap));
        long result = Long.parseLong(diffResult.trim());
        storageQuota.setData(ImmutableMap.of("aggregate_size", result));
        logger.info("query clickhouse traffic_aggregate success,{}", storageQuota);
        return storageQuota;
    }

//========================Druid==============================

    /**
     * 获取Druid当前存储大小 若获取 fail直接补0,Druid本身无法提供服务无需再query上次值
     */
    private StorageQuota getDruidCurr() {
        String currSql = "{\"query\":\"SELECT SUM(curr_size) AS curr_size FROM sys.servers WHERE server_type = 'historical'\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";
        StorageQuota storageQuota = new StorageQuota();
        String currResult = HttpClientUtils.httpPost(queryDruidUrl, currSql);
        List<Map<String, Object>> list = (List) JsonMapper.fromJsonString(currResult, List.class);
        Long currSize = Long.valueOf(String.valueOf(list.get(0).get("curr_size")));

        storageQuota.setData(ImmutableMap.of("used_size", currSize));
        logger.info("query druid used_size success,{}", storageQuota);
        return storageQuota;
    }

    /**
     * 获取Druid过去30秒增量 若获取 fail直接补0,Druid本身无法提供服务无需再query上次值
     */
    private StorageQuota getDruidDiff(Long currSize) {
        StorageQuota storageQuota = new StorageQuota();
        Long before = getCurrBefore(Constant.REPORT_AND_METRICS);
        long diff = getDiffNum(currSize, before);
        storageQuota.setData(ImmutableMap.of("aggregate_size", diff));
        logger.info("query druid aggregate_size success,{}", storageQuota);
        return storageQuota;
    }

    /**
     * 获取Druid最大存储大小 若获取 fail直接补0,Druid本身无法提供服务无需再query上次值
     */
    private StorageQuota getDruidMax() {
        String maxSql = "{\"query\":\"SELECT SUM(max_size) AS max_size FROM sys.servers WHERE server_type = 'historical'\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";
        StorageQuota storageQuota = new StorageQuota();
        String maxResult = HttpClientUtils.httpPost(queryDruidUrl, maxSql);
        List<Map<String, Long>> list = (List) JsonMapper.fromJsonString(maxResult, List.class);
        Long maxSize = list.get(0).get("max_size");
        storageQuota.setData(ImmutableMap.of("max_size", maxSize));
        logger.info("query druid max_size success,{}", storageQuota);
        return storageQuota;
    }
    /**
     * 获取hos前存储大小 若获取 failquerydruid内最新的值替补当前值
     */
    private StorageQuota getHBaseStorage(String queryHosUrl) {
        StorageQuota storageQuota = new StorageQuota();
        Header[] headers = {new BasicHeader(Constant.TOKEN, filesToken), new BasicHeader(HttpHeaders.CONTENT_TYPE, Constant.TEXT_XML)};
        String result = HttpClientUtils.httpGet(queryHosUrl.concat(FILE_STORAGE_PATH), headers);
        if("-1".equals(result)){
            throw new RuntimeException("hos server error");
        }
        HosSpace hosSpace = XmlUtil.converXmlToBean(HosSpace.class, result);

        Long hosCapacity = hosSpace.getHosCapacity();
        Long hosUsed = hosSpace.getHosUsed();
        if (hosCapacity==-1||hosUsed==-1){
            throw new RuntimeException("hos server error");
        }

        Map<String, Object> data  = new HashMap<>();
        data.put("max_size", hosCapacity);
        data.put("used_size", hosUsed);
        data.put("aggregate_size", getDiffNum(hosUsed, getCurrBefore(Constant.FILES)));

        storageQuota.setData(data);
        logger.info("query file storage success,{}", storageQuota);
        return storageQuota;
    }
//=======================工具类方法===============================

    /**
     * 用于通过druid获取上次对应类型的Curr值
     *
     * @param logType 统计类型
     * @return 上次的值
     */
    private Long getCurrBefore(String logType) {
        String currSql = "{\"query\":\"SELECT used_size FROM sys_storage_log WHERE log_type = '" + logType
                + "' ORDER BY __time DESC LIMIT 1\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";
        String currResult = HttpClientUtils.httpPost(queryDruidUrl, currSql);
        List<Map> list = (List) JsonMapper.fromJsonString(currResult, List.class);
        Long currSize = Long.valueOf(String.valueOf(list.get(0).get("used_size")));
        logger.info("query {} history used_size success,{}",logType, currSize);
        return currSize;
    }

    /**
     * 用于通过druid获取上次对应类型的Max值
     *
     * @param logType 统计类型
     * @return 上次的值
     */
/*    private Long getMaxBefore(String logType) {
        String maxSql = "{\"query\":\"SELECT max_size FROM sys_storage_log WHERE log_type = '" + logType
                + "' ORDER BY __time DESC LIMIT 1\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";
        String maxResult = HttpClientUtils.httpPost(queryDruidUrl, maxSql);
        List<Map> list = (List<Map>) JsonMapper.fromJsonString(maxResult, List.class);
        Long currSize = Long.valueOf(String.valueOf(list.get(0).get("max_size")));
        logger.info("query {} max_size history success,{}", logType,currSize);
        return currSize;
    }*/

    /**
     * @param node
     * @return 自定义的标准时间 last_storage,
     * 存在zookeeper 节点/storage/worker/+node
     */
    public Long getLastStorage(String node) {
        try {
            ZookeeperUtils zk = new ZookeeperUtils();
            //query
            String nodeData = zk.getNodeData(ZOOKEEPER_STORAGE_PATH + node, zookeeperServer);
            //不存在创建一个
            if (ObjectUtils.isEmpty(nodeData)) {
                Long lastTime = System.currentTimeMillis() / 1000;
                zk.modifyNode(ZOOKEEPER_STORAGE_PATH + node, String.valueOf(lastTime), zookeeperServer);
                return lastTime;
            }
            Long lastStorage = Long.valueOf(nodeData);
            logger.info("query standard time last_storage success,{}", lastStorage);

            return Long.valueOf(nodeData);
        } catch (Exception e) {
            logger.error("query standard time last_storage fail,{}", JobUtil.getErrorMsg(e));
            throw new BusinessException("query standard time last_storage fail ," + JobUtil.getErrorMsg(e));
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
        deleteParamMap.put("password", trafficPassword);
        deleteParamMap.put("user", trafficUserName);
        return deleteParamMap;
    }
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
        status.put("fileFailCount", getFileStorageJson(data, now));

        all.put("data", data);
        all.put("status", status);

        return all;
    }

    private int getCKStorageJson(List<Map> data, Long time) {
        int ckErrorCount = 0;
        try {
            StorageQuota clickHouseCurr = getClickHouseCurr();
            StorageQuota clickHouseMax = getClickHouseMax();

            if (clickHouseCurr.getStatus() != null || clickHouseMax.getStatus() != null) {
                XxlJobLogger.log("clickhouse storage error clickHouseCurr ={} , clickHouseMax={}", clickHouseCurr.getStatus(), clickHouseMax.getStatus());
                ckErrorCount++;
            }

            Map<String,Object> traffic = new HashMap<>();
            traffic.put("log_type", Constant.TRAFFIC_LOGS);
            traffic.put("time", time);
            traffic.put("last_storage", getLastStorage(Constant.ZK_TRAFFIC_LOGS));
            traffic.putAll(clickHouseCurr.getData());
            traffic.putAll(clickHouseMax.getData());
            data.add(traffic);
        } catch (Exception e) {
            logger.error("clickhouse storage error {}", JobUtil.getErrorMsg(e));
            XxlJobLogger.log("clickhouse storage error {}", JobUtil.getErrorMsg(e));
            ckErrorCount++;
        }
        return ckErrorCount;
    }

    private int getDruidStorageJson(List<Map> data, Long time) {
        int druidErrorCount = 0;
        try {
            StorageQuota druidCurr = getDruidCurr();
            StorageQuota druidDiff = getDruidDiff(Long.valueOf(String.valueOf(druidCurr.getData().get("used_size"))));
            StorageQuota druidMax = getDruidMax();
            druidErrorCount = 0;
            if (druidCurr.getStatus() != null || druidDiff.getStatus() != null || druidMax.getStatus() != null) {
                XxlJobLogger.log("druid storage error druidCurr ={} , druidDiff={}, druidMax={}", druidCurr.getStatus(), druidDiff.getStatus(), druidMax.getStatus());
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
        } catch (Exception e) {
            logger.error("druid storage error {}", JobUtil.getErrorMsg(e));
            XxlJobLogger.log("druid storage error {}", JobUtil.getErrorMsg(e));
            druidErrorCount++;
        }
        return druidErrorCount;
    }
    /*
     * 集群hbase存储配额
     **/
    private int getFileStorageJson(List<Map> data, Long time) {
        int errorCount = 0;
        try {
            List<String> queryHdfsUrlList = Splitter.on(",").trimResults().omitEmptyStrings().splitToList(filesHosServer);
            long max = 0;
            long used = 0;
            long aggregate = 0;
            for (String queryHdfsUrl : queryHdfsUrlList) {
                try {
                    queryHdfsUrl = Constant.HTTP.concat(queryHdfsUrl);
                    StorageQuota hosStorage = getHBaseStorage(queryHdfsUrl);
                    long maxSize = Long.parseLong(String.valueOf(hosStorage.getData().get("max_size")));
                    long usedSize = Long.parseLong(String.valueOf(hosStorage.getData().get("used_size")));
                    long aggregateSize = Long.parseLong(String.valueOf(hosStorage.getData().get("aggregate_size")));
                    max += maxSize;
                    used += usedSize;
                    aggregate += aggregateSize;
                } catch (Exception e) {
                    logger.error("file storage endpoint={},error={}", queryHdfsUrl, JobUtil.getErrorMsg(e));
                    XxlJobLogger.log("file storage endpoint={},error={}", queryHdfsUrl, JobUtil.getErrorMsg(e));
                    errorCount++;
                }
            }
            if(errorCount!=queryHdfsUrlList.size()){
                Map files = new HashMap<>();
                files.put("log_type", Constant.FILES);
                files.put("time", time);
                files.put("last_storage", getLastStorage(Constant.FILES));
                files.put("max_size", max);
                files.put("used_size", used);
                files.put("aggregate_size", aggregate);
                data.add(files);
            }
        } catch (Exception e) {
            logger.error("file storage error {}", JobUtil.getErrorMsg(e));
            XxlJobLogger.log("file storage error {}", JobUtil.getErrorMsg(e));
            errorCount++;
        }

        return errorCount;
    }
    /**
     * 用于组合ClickHouse一天执行一次的JSON串 特殊处理
     *
     * @return 结果json
     */
    public Map getDayJson() {
        Map all = new HashMap<>();
        List<Map> data = new ArrayList<>();
        //当clickhouse任务设置在第二天时，这个时间点为前一天的统计数据
        Long date = System.currentTimeMillis()/1000 - 3600*12;
        int ckFailCount = getCKStorageJson(data, date);

        StorageQuota clickHouseDiff = getClickHouseDiff();
        if (clickHouseDiff.getStatus() != null) {
            XxlJobLogger.log("query clickhouse storage error, clickHouseDiff ={} ", clickHouseDiff.getStatus());
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
     * 获取差值计算,若为负数则填写0
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
