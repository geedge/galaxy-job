package com.mesalab.executor.service;

import cn.hutool.log.Log;
import com.google.common.collect.Maps;
import com.mesalab.executor.core.config.StorgeConfig;
import com.mesalab.executor.core.utils.*;
import com.mesalab.executor.exception.BusinessException;
import com.mesalab.executor.pojo.HosSpace;
import com.xxl.job.core.log.XxlJobLogger;
import com.zdjizhi.utils.DateUtils;
import com.zdjizhi.utils.JsonMapper;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

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

    private Log logger = Log.get();

    private static final String ZOOKEEPER_STORAGE_PATH = "/storage/worker/";
    private static final String FILE_STORAGE_PATH = "/admin/diskspace";
    private static final String DRUID_PATH = "/druid/v2/sql";
    private static final int STORAGE_LIMITATION = 60; //查询历史数据时允许的最大间隔60 MINUTE

    @Value("${zookeeper.server}")
    private String zookeeperServer;
    private static  StorgeConfig storgeConfig =  (StorgeConfig)SpringContextUtil.getBean("storgeConfig");

    /**
     * 用于获取 ClickHouse 当前存储大小 若获取 failquerydruid内最新的值替补当前值
     */
    private Long getClickHouseCurr(String point) {
        Map<String, Object> deleteParamMap = getDeleteSource();
        String currSql = "SELECT SUM(`bytes_on_disk`) FROM " + storgeConfig.getSystemPartsCluster() + " WHERE database = '" + storgeConfig.getTrafficDatasource() + "';";
        deleteParamMap.put("query", currSql);
        String currResult = HttpClientUtils.httpGet(HttpClientUtils.getUrlWithParams(UrlUtil.getUrl(point), deleteParamMap));
        Long result = Long.valueOf(currResult.trim());
        logger.info("query clickhouse success, used_size={}", result);
        return result;

    }

    /**
     * 用于获取ClickHouse最大存储大小 若获取 failquerydruid内最新的值替补当前值
     */
    private Long getClickHouseMax(String point) {
        Map<String, Object> deleteParamMap = getDeleteSource();
        String maxSql = "SELECT SUM(`total_space`) FROM ".concat(storgeConfig.getSystemDisks()).concat(";");
        deleteParamMap.put("query", maxSql);
        String maxResult = HttpClientUtils
                .httpGet(HttpClientUtils.getUrlWithParams(UrlUtil.getUrl(point), deleteParamMap));
        long result = Long.parseLong(maxResult.trim());
        logger.info("query clickhouse max_size success ,{}", result);
        return result;
    }

    /**
     * 用于获取ClickHouse 差值 若获取 fail直接写入0
     */
    private Long getClickHouseDiff(String point) {
        String date = DateUtils.getDateOfYesterday("yyyyMMdd");
        Map<String, Object> deleteParamMap = getDeleteSource();
        String diffSql = "SELECT SUM(bytes_on_disk) FROM ".concat(storgeConfig.getSystemPartsCluster()).concat(" WHERE database = '" ).concat(storgeConfig.getTrafficDatasource())
                .concat("' AND partition = '").concat(date).concat("';");
        deleteParamMap.put("query", diffSql);
        String diffResult = HttpClientUtils
                .httpGet(HttpClientUtils.getUrlWithParams(UrlUtil.getUrl(point), deleteParamMap));
        long result = Long.parseLong(diffResult.trim());
        logger.info("query clickhouse success aggregate_size={}", result);
        return result;
    }

//========================Druid==============================

    /**
     * 获取Druid当前存储大小 若获取 fail直接补0,Druid本身无法提供服务无需再query上次值
     */
    private Long getDruidUsed(String point) {
        String currSql = "{\"query\":\"SELECT SUM(size) AS used_size FROM sys.segments WHERE datasource NOT LIKE '%hot%' and is_available=1\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";
        String currResult = HttpClientUtils.httpPost(UrlUtil.getUrl(point).concat(DRUID_PATH), currSql);
        List<Map<String, Object>> list = (List) JsonMapper.fromJsonString(currResult, List.class);
        if(list!=null&&list.size()==0){
            return 0L;
        }
        Long currSize = 0L;
        if(list!=null&&list.get(0)!=null){
            Map<String, Object> map = list.get(0);
            if(map!=null&&map.get("used_size")!=null){
                currSize = Long.valueOf(String.valueOf(map.get("used_size")));

            }
        }
        logger.info("query druid used_size success,{}", currSize);
        return currSize;
    }

    /**
     * 获取Druid最大存储大小 若获取 fail直接补0,Druid本身无法提供服务无需再query上次值
     */
    private Long getDruidMax(String point) {
        String maxSql = "{\"query\":\"SELECT SUM(max_size) AS max_size FROM sys.servers WHERE server_type = 'historical'\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";
        String maxResult = HttpClientUtils.httpPost(UrlUtil.getUrl(point).concat(DRUID_PATH), maxSql);
        List<Map<String, Object>> list = (List) JsonMapper.fromJsonString(maxResult, List.class);
        Long maxSize = 0L;
        if(list!=null&&list.get(0)!=null){
            Map<String, Object> map = list.get(0);
            if(map!=null&&map.get("max_size")!=null){
                maxSize = Long.valueOf(String.valueOf(map.get("max_size")));

            }
        }
        logger.info("query druid max_size success,{}", maxSize);
        return maxSize;
    }
    /**
     * 获取hos前存储大小 若获取 failquerydruid内最新的值替补当前值
     */
    private Map getHBaseStorage(String key,String point) {
        Header[] headers = {new BasicHeader(Constant.TOKEN, storgeConfig.getFilesToken()), new BasicHeader(HttpHeaders.CONTENT_TYPE, Constant.TEXT_XML)};
        String result = HttpClientUtils.httpGet(UrlUtil.getUrl(point).concat(FILE_STORAGE_PATH), headers);
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
        data.put("aggregate_size", getDiffNum(hosUsed, getCurrBefore(Constant.FILES,key)));

        logger.info("query file storage success,{}", data);
        return data;
    }
//=======================工具类方法===============================

    /**
     * 用于通过druid获取上次对应类型的Curr值
     *
     * @param logType 统计类型
     * @return 上次的值,如果不是当天，也不是在00:00:00则返回0
     * //如果历史记录时间比现在少于1个小时，写0，如果最近一小时为0则查今天的
     */
    private Long getCurrBefore(String logType, String key) {
        String currSql = "{\"query\":\"SELECT used_size FROM sys_storage_log WHERE log_type = '" + logType
                + "' and data_center = '"+ key +"' and __time >= CURRENT_TIMESTAMP - INTERVAL '"+ STORAGE_LIMITATION +"' MINUTE ORDER BY __time DESC LIMIT 1\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";
        String currResult = HttpClientUtils.httpPost(UrlUtil.getUrl(storgeConfig.getAnalyticServer()).concat(DRUID_PATH), currSql);
        List<Map> list = (List) JsonMapper.fromJsonString(currResult, List.class);
        if(list!=null&&list.size()==0){
            return 0L;
        }
        Long historyUsed = 0L;
        if(list!=null&&list.get(0)!=null){
            Map<String, Object> map = list.get(0);
            if(map!=null&&map.get("used_size")!=null){
                historyUsed = Long.valueOf(String.valueOf(map.get("used_size")));

            }
        }
        logger.info("query {} history used_size success,{}",logType, historyUsed);
        return historyUsed;
    }

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
        } catch (BusinessException e) {
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
        deleteParamMap.put("password", storgeConfig.getTrafficUserKey());
        deleteParamMap.put("user", storgeConfig.getTrafficUsername());
        return deleteParamMap;
    }
    /**
     * 用于组合正常维度的JSON串
     *
     * @return 结果json
     */
    public Map<String, Map> getDiskJson(boolean ckDay) {

        Map all = new HashMap<>();
        List<Map> data = new ArrayList<>();
        Long now = System.currentTimeMillis() / 1000;
        Map<String, Integer> status = new HashMap<>();
        if(ckDay){
            //当clickhouse任务设置在第二天时，这个时间点为前一天的统计数据
            long time = 12*3600L;
            long timestamp = DateUtils.convertStringToTimestamp(DateUtils.getDateOfYesterday("yyyyMMdd"), "yyyyMMdd")+time;
            status.put("trafficFailCount", getCKStorageJson(data, timestamp,true));
        } else{
            status.put("trafficFailCount", getCKStorageJson(data, now,false));
            status.put("reportFailCount", getDruidStorageJson(data, now));
            status.put("fileFailCount", getFileStorageJson(data, now));
        }
        all.put("data", data);
        all.put("status", status);
        return all;
    }
    private int getCKStorageJson(List<Map> data, Long time, boolean day) {
        int errorCount = 0;
        try {
            for (Map.Entry<String, String> dc : storgeConfig.getTrafficDataCenter().entrySet()) {
                try {
                    Map<String,Object> traffic = new HashMap<>();
                    traffic.put("log_type", Constant.TRAFFIC_LOGS);
                    traffic.put("last_storage", getLastStorage(Constant.ZK_TRAFFIC_LOGS));
                    traffic.put("data_center",dc.getKey());
                    traffic.put("time", time);
                    traffic.put("used_size",getClickHouseCurr(dc.getValue()));
                    traffic.put("max_size",getClickHouseMax(dc.getValue()));
                    if(day){
                        traffic.put("aggregate_size",getClickHouseDiff(dc.getValue()));
                    }
                    data.add(traffic);
                } catch (BusinessException e) {
                    logger.error("clickhouse storage error {}", JobUtil.getErrorMsg(e));
                    XxlJobLogger.log("clickhouse storage error {}", JobUtil.getErrorMsg(e));
                    errorCount++;
                }
            }
        } catch (BusinessException e) {
            logger.error("clickhouse storage error {}", JobUtil.getErrorMsg(e));
            XxlJobLogger.log("clickhouse storage error {}", JobUtil.getErrorMsg(e));
            errorCount++;
        }
        return errorCount;
    }

    private int getDruidStorageJson(List<Map> data, Long time) {
        int errorCount = 0;
        try {
            for (Map.Entry<String, String> dc : storgeConfig.getAnalyticDataCenter().entrySet()) {
                try {
                    Map metrics = new HashMap<>();
                    metrics.put("log_type", Constant.REPORT_AND_METRICS);
                    metrics.put("time", time);
                    metrics.put("last_storage", getLastStorage(Constant.ZK_REPORT_AND_METRICS));
                    metrics.put("data_center", dc.getKey());

                    metrics.put("max_size",getDruidMax(dc.getValue()));
                    Long druidUsed = getDruidUsed(dc.getValue());
                    metrics.put("used_size",druidUsed);
                    metrics.put("aggregate_size", getDiffNum(druidUsed, getCurrBefore(Constant.REPORT_AND_METRICS,dc.getKey())));
                    data.add(metrics);
                } catch (BusinessException e) {
                    logger.error("druid storage endpoint={}, error {}",dc.getValue(), JobUtil.getErrorMsg(e));
                    XxlJobLogger.log("druid storage endpoint={},error {}",dc.getValue(), JobUtil.getErrorMsg(e));
                    errorCount++;
                }
            }
        } catch (BusinessException e) {
            logger.error("druid storage error {}", JobUtil.getErrorMsg(e));
            XxlJobLogger.log("druid storage error {}", JobUtil.getErrorMsg(e));
            errorCount++;
        }

        return errorCount;
    }
    /*
     * 集群hbase存储配额
     **/
    private int getFileStorageJson(List<Map> data, Long time) {
        int errorCount = 0;
        try {
            for (Map.Entry<String, String> entry : storgeConfig.getFilesDataCenter().entrySet()) {
                try {
                    Map files = new HashMap<>();
                    files.put("log_type", Constant.FILES);
                    files.put("time", time);
                    files.put("last_storage", getLastStorage(Constant.FILES));
                    files.put("data_center",entry.getKey());
                    files.putAll(getHBaseStorage(entry.getKey(),entry.getValue()));
                    data.add(files);
                } catch (BusinessException e) {
                    logger.error("file storage endpoint={},error={}", entry.getValue(), JobUtil.getErrorMsg(e));
                    XxlJobLogger.log("file storage endpoint={},error={}", entry.getValue(), JobUtil.getErrorMsg(e));
                    errorCount++;
                }
            }
        } catch (BusinessException e) {
            logger.error("file storage error {}", JobUtil.getErrorMsg(e));
            XxlJobLogger.log("file storage error {}", JobUtil.getErrorMsg(e));
            errorCount++;
        }

        return errorCount;
    }

    /**
     * 获取差值计算,若为负数则填写0
     *
     * @param now    这一次的值
     * @param before 上一次的值
     * @return 差值
     */
    private Long getDiffNum(Long now, Long before) {
        if(before<=0){
            return 0L;
        }
        long diff = now - before;
        if (diff >= 0) {
            return diff;
        } else {
            return 0L;
        }
    }

}
