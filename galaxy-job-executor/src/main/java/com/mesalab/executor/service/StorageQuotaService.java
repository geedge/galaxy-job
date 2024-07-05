package com.mesalab.executor.service;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.db.handler.NumberHandler;
import cn.hutool.db.sql.SqlExecutor;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import com.geedgenetworks.utils.DateUtils;
import com.google.common.collect.Lists;
import com.mesalab.executor.core.config.StorgeConfig;
import com.mesalab.executor.core.utils.*;
import com.mesalab.executor.exception.BusinessException;
import com.mesalab.executor.jobhandler.LogStorageQuotaJob;
import com.mesalab.executor.pojo.HosSpace;
import com.mesalab.executor.pojo.JDBCParam;
import com.mesalab.executor.pojo.JobResult;
import com.mesalab.executor.pojo.SysStorageEvent;
import com.xxl.job.core.biz.model.ReturnT;
import lombok.Data;
import org.apache.groovy.util.Maps;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import javax.annotation.Resource;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @Description:
 * @Author: zhq
 * @CreateDate: 2024/6/13
 * @Version: 1.0
 */

@Service
public class StorageQuotaService {

    private StorgeConfig storgeConfig = (StorgeConfig) SpringContextUtil.getBean("storgeConfig");
    private Log logger = Log.get();

    //查询历史数据时允许的最大间隔second
    private static final int MAX_QUERY_INTERVAL = 2 * 60 * 60;
    private final String clickhouseMaxSizeSql = StrUtil.format("SELECT SUM(`total_space`) FROM {};", storgeConfig.getSystemDisks());
    private final String clickhouseUsedSizeSql = StrUtil.format("SELECT SUM(`bytes_on_disk`) FROM {} WHERE database = '{}' and active = 1;", storgeConfig.getSystemPartsCluster(), storgeConfig.getTrafficDatasource());

    private final String druidMaxSizeSql = "{\"query\":\"SELECT SUM(max_size) AS max_size FROM sys.servers WHERE server_type = 'historical'\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"csv\"}";
    private final String druidUsedSizeSql = "{\"query\":\"SELECT SUM(size) AS used_size FROM sys.segments WHERE datasource NOT LIKE '%hot%' and is_published = 1 \",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"csv\"}";

    @Value("${zookeeper.server}")
    private String zookeeperServer;

    @Resource
    private LogStorageQuotaJob logStorageQuotaJob;

    public int getAndSaveStorageQuotaInfo(String[] logTypes, JDBCParam jdbcParam, boolean containBytes) {

        int failCount = 0;
        for (String logType : logTypes) {
            JobResult<SysStorageEvent> jobResult = new JobResult<>();
            try {
                switch (logType) {
                    case Constant.TRAFFIC_LOGS:
                        jobResult = getStorageQuota(jdbcParam, containBytes, storgeConfig.getTrafficDataCenter(), logType, Constant.ZK_TRAFFIC_LOGS);
                        break;
                    case Constant.METRICS:
                        jobResult = getStorageQuota(jdbcParam, containBytes, storgeConfig.getAnalyticDataCenter(), logType, Constant.ZK_REPORT_AND_METRICS);
                        break;
                    case Constant.FILES:
                        jobResult = getStorageQuota(jdbcParam, containBytes, storgeConfig.getFilesDataCenter(), logType, Constant.FILES);
                        break;
                    default:
                        break;
                }
                failCount += jobResult.getFailCount();
                if (containBytes) {
                    setStorageAvg(jobResult);
                }
                DBUtils.save(jobResult.getData(), jdbcParam);
            } catch (Exception e) {
                failCount++;
                JobUtil.errorLog(e.getMessage());
            }
        }
        return failCount;
    }

    public int deleteOldLog(String logType, int maxUsage, int minIntervalMinutes) {

        int failCount = 0;
        try {
            switch (logType) {
                case Constant.TRAFFIC_LOGS:
                    failCount += deleteOldLogs(storgeConfig.getTrafficDataCenter(), maxUsage, minIntervalMinutes,
                            host -> getUsage(getClickhouseStorageInfo(host)), logStorageQuotaJob::deleteTrafficDataByCluster, Constant.ZK_TRAFFIC_LOGS);
                    break;
                case Constant.METRICS:
                    failCount += deleteOldLogs(storgeConfig.getAnalyticDataCenter(), maxUsage, minIntervalMinutes,
                            host -> getUsage(getDruidStorageInfo(host)), logStorageQuotaJob::deleteReportAndMetricsData, Constant.ZK_REPORT_AND_METRICS);
                    break;
                case Constant.FILES:
                    failCount += deleteOldLogs(storgeConfig.getFilesDataCenter(), maxUsage, minIntervalMinutes,
                            host -> getUsage(getHosStorageInfo(host)), logStorageQuotaJob::deleteFiles, Constant.FILES);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            failCount++;
            JobUtil.errorLog(e.getMessage());
        }
        return failCount;
    }

    /**
     * @param dataCenterMap
     * @param maxUsage
     * @param minIntervalMinutes
     * @param getUsageFunction
     * @param deleteFunction
     * @param zkPath             // 1.获取删除天数 /datacenter maxDays = now - data_center_last_storage - 1
     *                           // 2.空则使用 maxDays = now - last_storage - 1
     *                           // 3.调用删除接口 参数maxDays, 记录日志
     *                           // 4.修改 datacenter时间
     */
    private int deleteOldLogs(Map<String, String> dataCenterMap, int maxUsage, int minIntervalMinutes,
                              Function<String, Double> getUsageFunction, Function<String, ReturnT<String>> deleteFunction, String zkPath) {
        int failCount = 0;
        for (Map.Entry<String, String> entry : dataCenterMap.entrySet()) {
            try {
                String dataCenterHost = entry.getValue();
                String dataCenterName = entry.getKey();
                String zkClearTimePath = StrUtil.join("/", zkPath, dataCenterName, Constant.ZOOKEEPER_STORAGE_CLEAR_TIME);

                if (!checkStorageInterval(zkClearTimePath, minIntervalMinutes)) {
                    continue;
                }
                double usage = getUsageFunction.apply(dataCenterHost);
                if (maxUsage <= usage * 100d) {
                    String zkDcLastStoragePath = StrUtil.join("/", zkPath, dataCenterName);
                    Long lastStorageTime = getLastStorage(zkDcLastStoragePath);
                    if (lastStorageTime == null) {
                        lastStorageTime = getLastStorage(zkPath);
                        if (lastStorageTime == null) {
                            continue;
                        }
                    }
                    long storageTime = lastStorageTime + Constant.ONE_DAY;
                    long maxDays = DateUtil.betweenDay(DateUtil.date(storageTime * 1000), DateUtil.date(), true);

                    JobUtil.infoLog("delete old log maxDays: {}", maxDays);

                    ReturnT<String> deleteResult = deleteFunction.apply(JSONUtil.toJsonStr(Maps.of(Constant.MAX_DAYS, maxDays)));
                    if (deleteResult.getCode() != 200) {
                        failCount++;
                        JobUtil.errorLog("{} log delete fail {}", dataCenterName, deleteResult.getMsg());
                        continue;
                    }
                    setLastStorage(zkDcLastStoragePath, storageTime);
                    setLastStorage(zkClearTimePath, DateUtil.currentSeconds());
                    JobUtil.infoLog("{} log delete success", dataCenterName);
                    logger.warn("{} log delete success", dataCenterName);
                }
            } catch (Exception e) {
                failCount++;
                JobUtil.errorLog("delete log error {}", e.getMessage());
            }
        }
        return failCount;
    }

    private boolean checkStorageInterval(String zkClearTimePath, int minIntervalMinutes) {
        long now = DateUtil.currentSeconds();
        Long lastStorage = getLastStorage(zkClearTimePath);
        if (lastStorage != null && now - lastStorage < minIntervalMinutes * 60) {
            JobUtil.infoLog("The deletion task is being executed");
            return false;
        }
        return true;
    }

    private double getUsage(StorageSizeInfo storageSizeInfo) {
        Long totalSize = storageSizeInfo.getTotalSize();
        Long usedSize = storageSizeInfo.getUsedSize();
        double usage = usedSize / (double) totalSize;
        usage = Double.valueOf(String.format("%.4f", usage));
        JobUtil.infoLog("total size {}, used size {}, usage {}", totalSize, usedSize, usage);
        return usage;
    }

    /**
     * 将一天的数据平均分为每5分钟的数据
     *
     * @param jobResult
     */
    private void setStorageAvg(JobResult<SysStorageEvent> jobResult) {

        long interval = 60;
        long num = 24 * 60 / interval;

        List<SysStorageEvent> newResultList = Lists.newArrayList();
        long timestamp = DateUtils.convertStringToTimestamp(DateUtils.getDateOfYesterday("yyyyMMdd"), "yyyyMMdd");
        for (SysStorageEvent sysStorageEvent : jobResult.getData()) {
            Long dayBytes = sysStorageEvent.getBytes();
            long minBytes = dayBytes / num;
            for (long i = 0; i < num; i++) {
                SysStorageEvent newStorageEvent = sysStorageEvent;
                newStorageEvent.setBytes(minBytes);

                timestamp = timestamp + interval * 60;
                newStorageEvent.setGeneratedTime(timestamp);
                newResultList.add(newStorageEvent);
            }
        }
        jobResult.setData(newResultList);
        JobUtil.infoLog("set storage data ... ");
    }

    private JobResult<SysStorageEvent> getStorageQuota(JDBCParam jdbcParam, boolean containIncrease, Map<String, String> dataCenterMap, String logType, String zkNode) {
        List<SysStorageEvent> sysStorageEvents = Lists.newArrayList();
        JobResult<SysStorageEvent> jobResult = new JobResult<>();
        int failCount = 0;
        long generatedTime = DateUtil.currentSeconds();
        Long sinceTime = getLastStorage(zkNode);
        for (Map.Entry<String, String> entry : dataCenterMap.entrySet()) {
            try {
                String dataCenterHost = entry.getValue();
                String dataCenterName = entry.getKey();
                StorageSizeInfo storageSizeInfo = getStorageSizeInfo(dataCenterHost, logType);

                Long bytes = containIncrease ? getIncreaseSize(jdbcParam, storageSizeInfo.getUsedSize(), entry.getKey(), logType) : 0L;

                SysStorageEvent storageEvent = SysStorageEvent.builder()
                        .logType(logType)
                        .dataCenter(dataCenterName)
                        .generatedTime(generatedTime)
                        .totalAllocatedSize(storageSizeInfo.getTotalSize())
                        .usedSize(storageSizeInfo.getUsedSize())
                        .bytes(bytes)
                        .sinceTime(sinceTime)
                        .build();

                sysStorageEvents.add(storageEvent);
                JobUtil.infoLog("Get {} storage info {}:", logType, storageEvent.toString());
            } catch (Exception e) {
                failCount++;
                JobUtil.errorLog("Get {} storage error : datacenter {}, message {}", logType, entry.getKey(), e.getMessage());
            }
        }
        jobResult.setFailCount(failCount);
        jobResult.setData(sysStorageEvents);
        return jobResult;
    }

    private StorageSizeInfo getStorageSizeInfo(String dataCenterHost, String logType) {
        StorageSizeInfo storageSizeInfo = null;
        switch (logType) {
            case Constant.TRAFFIC_LOGS:
                storageSizeInfo = getClickhouseStorageInfo(dataCenterHost);
                break;
            case Constant.METRICS:
                storageSizeInfo = getDruidStorageInfo(dataCenterHost);
                break;
            case Constant.FILES:
                storageSizeInfo = getHosStorageInfo(dataCenterHost);
                break;
            default:
                break;
        }
        return storageSizeInfo;
    }

    private Long getIncreaseSize(JDBCParam jdbcParam, Long usedSize, String datacenter, String logType) throws Exception {
        final String lastUsedSizeSql = "SELECT used_size FROM " + jdbcParam.getTable() + " WHERE log_type = '" + logType + "' and data_center = '" + datacenter
                + "' and generated_time >=  UNIX_TIMESTAMP() - " + MAX_QUERY_INTERVAL + " ORDER BY generated_time DESC LIMIT 1;";
        JobUtil.infoLog(lastUsedSizeSql);

        try (Connection conn = DBUtils.getDBConn(jdbcParam)) {
            Number lastUsedSize = SqlExecutor.query(conn, lastUsedSizeSql, new NumberHandler());
            if (lastUsedSize == null || lastUsedSize.longValue() <= 0 || usedSize - lastUsedSize.longValue() <= 0) {
                return 0L;
            }

            return usedSize - lastUsedSize.longValue();
        } catch (Exception e) {
            throw e;
        }
    }

    public Long queryClickhouse(String datacenterHost, Map<String, Object> ckParamMap) {
        String result = HttpClientUtils.httpGet(HttpClientUtils.getUrlWithParams(UrlUtil.getUrl(datacenterHost), ckParamMap));
        if ("-1".equals(result)) {
            throw new BusinessException("Get clickhouse http fail -1");
        }
        return Long.valueOf(result.trim());
    }

    public Long queryDruid(String datacenterHost, String usedSizeSql) {
        final String druidPath = "/druid/v2/sql";
        String result = HttpClientUtils.httpPost(UrlUtil.getUrl(datacenterHost).concat(druidPath), usedSizeSql);

        if ("-1".equals(result)) {
            throw new BusinessException("Get druid http fail -1");
        }
        return Long.valueOf(result.trim());
    }

    public StorageSizeInfo getClickhouseStorageInfo(String datacenterHost) {
        Map<String, Object> ckParamMap = storgeConfig.getCkSource();
        // 1. 总计
        ckParamMap.put("query", clickhouseMaxSizeSql);
        Long totalSize = queryClickhouse(datacenterHost, ckParamMap);
        //2. 已使用
        ckParamMap.put("query", clickhouseUsedSizeSql);
        Long usedSize = queryClickhouse(datacenterHost, ckParamMap);
        usedSize = usedSize > totalSize ? totalSize : usedSize;
        StorageSizeInfo storageSizeInfo = new StorageSizeInfo();
        storageSizeInfo.setTotalSize(totalSize);
        storageSizeInfo.setUsedSize(usedSize);
        return storageSizeInfo;
    }

    public StorageSizeInfo getDruidStorageInfo(String datacenterHost) {
        // 1. 总计
        Long totalSize = queryDruid(datacenterHost, druidMaxSizeSql);
        //2. 已使用
        Long usedSize = queryDruid(datacenterHost, druidUsedSizeSql);
        usedSize = usedSize > totalSize ? totalSize : usedSize;

        StorageSizeInfo storageSizeInfo = new StorageSizeInfo();
        storageSizeInfo.setTotalSize(totalSize);
        storageSizeInfo.setUsedSize(usedSize);
        return storageSizeInfo;
    }

    public StorageSizeInfo getHosStorageInfo(String datacenterHost) {
        final String fileStoragePath = "/admin/diskspace";
        Header[] headers = {new BasicHeader(Constant.TOKEN, storgeConfig.getFilesToken()),
                new BasicHeader(HttpHeaders.CONTENT_TYPE, Constant.TEXT_XML)};
        String result = HttpClientUtils.httpGet(UrlUtil.getUrl(datacenterHost).concat(fileStoragePath), headers);

        if ("-1".equals(result)) {
            throw new BusinessException("Get hos http request fail -1 , server: " + datacenterHost);
        }
        HosSpace hosSpace = XmlUtil.converXmlToBean(HosSpace.class, result);
        Long totalSize = hosSpace.getHosCapacity();
        Long usedSize = hosSpace.getHosUsed();
        usedSize = usedSize > totalSize ? totalSize : usedSize;
        StorageSizeInfo storageSizeInfo = new StorageSizeInfo();
        storageSizeInfo.setTotalSize(totalSize);
        storageSizeInfo.setUsedSize(usedSize);
        return storageSizeInfo;
    }

    /**
     * @param node
     * @return 自定义的标准时间 last_storage,
     * 存在zookeeper 节点/storage/worker/+node
     */
    private Long getLastStorage(String node) {
        try {
            ZookeeperUtils zk = new ZookeeperUtils();
            String nodeData = zk.getNodeData(Constant.ZOOKEEPER_STORAGE_PATH + node, zookeeperServer);
            if (ObjectUtils.isEmpty(nodeData)) {
                return null;
            }
            Long lastStorage = Long.valueOf(nodeData);
            logger.info("query standard time last_storage success,{}", lastStorage);
            return Long.valueOf(nodeData);
        } catch (Exception e) {
            throw new BusinessException("query standard time last_storage fail ," + JobUtil.getErrorMsg(e));
        }

    }

    private void setLastStorage(String path, Long lastStorage) {
        ZookeeperUtils zk = new ZookeeperUtils();
        zk.modifyNode(Constant.ZOOKEEPER_STORAGE_PATH + path, String.valueOf(lastStorage), zookeeperServer);
    }

    @Data
    class StorageSizeInfo {
        private Long totalSize;
        private Long usedSize;

    }

}
