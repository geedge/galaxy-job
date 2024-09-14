package com.mesalab.executor.service;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.db.handler.BeanListHandler;
import cn.hutool.db.handler.NumberHandler;
import cn.hutool.db.sql.SqlExecutor;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import com.geedgenetworks.utils.DateUtils;
import com.google.common.collect.Lists;
import com.mesalab.executor.core.config.StorgeConfig;
import com.mesalab.executor.core.utils.*;
import com.mesalab.executor.exception.BusinessException;
import com.mesalab.executor.jobhandler.LogStorageQuotaJob;
import com.mesalab.executor.pojo.*;
import com.xxl.job.core.biz.model.ReturnT;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.Data;
import org.apache.groovy.util.Maps;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;
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

    private final String clickhouseMaxSizeSql = StrUtil.format("SELECT SUM(`total_space`) FROM {};", storgeConfig.getSystemDisks());
    private final String clickhouseUsageSql = StrUtil.format("SELECT 1-(sum(unreserved_space)/sum(total_space)) AS usage FROM {};", storgeConfig.getSystemDisks());
    private final String clickhouseUsedSizeSql = StrUtil.format("SELECT SUM(`bytes_on_disk`) FROM {} WHERE database = '{}' and active = 1;", storgeConfig.getSystemPartsCluster(), storgeConfig.getTrafficDatasource());
    private final String clickhouseDailyUsedSizeSql = StrUtil.format("SELECT DISTINCT(`partition`) as date, SUM(`bytes_on_disk`) as bytes from {} WHERE database = '{}' {} and date <=  formatDateTime(now(),'%Y%m%d') and active = 1 group by date FORMAT JSON;",
            storgeConfig.getSystemPartsCluster(), storgeConfig.getTrafficDatasource());


    private final String druidMaxSizeSql = "{\"query\":\"SELECT SUM(max_size) AS max_size FROM sys.servers WHERE server_type = 'historical'\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"csv\"}";
    private final String druidUsedSizeSql = "{\"query\":\"SELECT SUM(size) AS used_size FROM sys.segments WHERE is_published = 1 \",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"csv\"}";
    //    private final String druidUsedActiveSizeSql = "{\"query\":\"SELECT SUM(size) AS used_size FROM sys.segments WHERE is_published = 1 and is_active=1\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"csv\"}";
    private final String mysqlDailyUsedSizeSql = "SELECT UNIX_TIMESTAMP(FROM_UNIXTIME(generated_time,'%Y%m%d')) AS date, sum(bytes) AS bytes FROM {}.{} WHERE generated_time <= CURRENT_TIMESTAMP AND generated_time > UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 1 YEAR)) AND  log_type='{}' AND data_center='{}' GROUP BY date, log_type ORDER BY date DESC";
    private final String druidDailyUsedSizeSql = "{\"query\":\"SELECT \\\"start\\\" as \\\"date\\\", sum(\\\"size\\\") as bytes from sys.segments where is_published = 1 and \\\"start\\\" < '3000' {} group by \\\"start\\\" ORDER BY \\\"start\\\" \",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";


    @Value("${zookeeper.server}")
    private String zookeeperServer;

    @Resource
    private LogStorageQuotaJob logStorageQuotaJob;

    private Counter counterTrafficLogsDelete;
    private Counter counterMetricsDelete;
    private Counter counterFilesDelete;


    @Resource
    MeterRegistry meterRegistry;

    @PostConstruct
    private void init() {
        String monitorName = "delete_old_log";
        String monitorTag = "log_type";
        counterTrafficLogsDelete = meterRegistry.counter(monitorName, monitorTag, Constant.TRAFFIC_LOGS);
        counterMetricsDelete = meterRegistry.counter(monitorName, monitorTag, Constant.METRICS);
        counterFilesDelete = meterRegistry.counter(monitorName, monitorTag, Constant.FILES);


    }

    public int getAndSaveStorageQuotaInfo(String[] logTypes, JDBCParam jdbcParam, boolean containBytes) {
        int failCount = 0;
        for (String logType : logTypes) {
            JobResult<SysStorageEvent> jobResult = new JobResult<>();
            try {
                JobUtil.infoLog("--------------Get {} storage info task.-----------", logType);
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

    public int deleteOldLog(String logType, int durationMinutes, int minIntervalMinutes, JDBCParam jdbcParam) {

        int failCount = 0;
        try {
            JobUtil.infoLog("--------------Delete {} old log task.--------------------", logType);
            switch (logType) {
                case Constant.TRAFFIC_LOGS:
                    failCount += deleteOldLogs(storgeConfig.getTrafficDataCenter(), durationMinutes, minIntervalMinutes, jdbcParam, logType,
                            this::getClickhouseStorageInfoByUnreserved, logStorageQuotaJob::deleteTrafficData, counterTrafficLogsDelete::increment, Constant.ZK_TRAFFIC_LOGS);
                    break;
                case Constant.METRICS:
                    failCount += deleteOldLogs(storgeConfig.getAnalyticDataCenter(), durationMinutes, minIntervalMinutes, jdbcParam, logType,
                            this::getDruidStorageInfo, logStorageQuotaJob::deleteReportAndMetricsData, counterMetricsDelete::increment, Constant.ZK_REPORT_AND_METRICS);
                    break;
                case Constant.FILES:
                    failCount += deleteOldLogs(storgeConfig.getFilesDataCenter(), durationMinutes, minIntervalMinutes, jdbcParam, logType,
                            this::getHosStorageInfo, logStorageQuotaJob::deleteFiles, counterFilesDelete::increment, Constant.FILES);
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
     * @param minIntervalMinutes
     * @param getStorageInfo
     * @param deleteFunction
     * @param zkPath
     */
    private int deleteOldLogs(Map<String, String> dataCenterMap, int durationMinutes, int minIntervalMinutes, JDBCParam jdbcParam, String logType,
                              Function<String, StorageSizeInfo> getStorageInfo,
                              Function<String, ReturnT<String>> deleteFunction,
                              Consumer<Double> setMonitor,
                              String zkPath) throws Exception {
        // 1.获取删除天数
        // 2.空则使用
        // 3.调用删除接口 参数maxDays, 记录日志
        // 4.修改 datacenter时间
        StorageQuota storageQuota = getStorageQuota(logType);
        double maxUsage = storageQuota.getMax_usage() / 100D;

        int failCount = 0;
        for (Map.Entry<String, String> entry : dataCenterMap.entrySet()) {
            try {
                String dataCenterHost = entry.getValue();
                String dataCenterName = entry.getKey();
                String zkClearTimePath = StrUtil.join("/", zkPath, dataCenterName, Constant.ZOOKEEPER_STORAGE_CLEAR_TIME);

                // 距离上次删除最小时间
                if (!checkStorageInterval(zkClearTimePath, minIntervalMinutes)) {
                    continue;
                }

                StorageSizeInfo storageSizeInfo = getStorageInfo.apply(dataCenterHost);
                boolean overMaxUsage = maxUsage <= storageSizeInfo.getUsage();
                // 距离上次达到时间
                if (!checkOverMaxUsageDuration(overMaxUsage, zkPath, dataCenterName, durationMinutes)) {
                    continue;
                }
                if (overMaxUsage) {

                    List<DailyStorage> historyStorageDaily = getHistoryStorageDaily(jdbcParam, logType, dataCenterName, dataCenterHost);

                    Long deleteDate = getDeleteDate(historyStorageDaily, storageSizeInfo.getTotalSize(), maxUsage, zkPath, dataCenterName);
                    long maxDays = DateUtil.betweenDay(DateUtil.date(deleteDate * 1000), DateUtil.date(), true);
                    maxDays = maxDays < 1 ? 1 : maxDays;

                    JobUtil.infoLog("delete old log date : {}, maxDays: {}", DateUtil.date(deleteDate * 1000), maxDays);
                    ReturnT<String> deleteResult = deleteFunction.apply(JSONUtil.toJsonStr(Maps.of(Constant.MAX_DAYS, maxDays)));

                    if (deleteResult.getCode() != 200) {
                        failCount++;
                        JobUtil.errorLog("{} log delete fail {}", dataCenterName, deleteResult.getMsg());
                        continue;
                    }
                    setZkStorageNode(StrUtil.join("/", zkPath, dataCenterName), deleteDate);
                    setZkStorageNode(zkClearTimePath, DateUtil.currentSeconds());

                    setMonitor.accept(1D);
                    JobUtil.warnLog("{} log delete success", dataCenterName);
                }
            } catch (Exception e) {
                failCount++;
                JobUtil.errorLog("delete log error {}", e.getMessage());
            }
        }
        return failCount;
    }

    private boolean checkOverMaxUsageDuration(boolean overMaxUsage, String zkPath, String dataCenterName, int durationMinutes) {
        String zkDurationTimePath = StrUtil.join("/", zkPath, dataCenterName, Constant.ZOOKEEPER_STORAGE_DURATION_TIME);
        String zkOverMaxUsagePath = StrUtil.join("/", zkPath, dataCenterName, Constant.ZOOKEEPER_STORAGE_OVER_MAX_USAGE);

        // 1. 超限：获取记录，如为0，则改为1，获取持续时间，是否超时：超时: true,未超时: false
        // 2. 未超限：false
        if (overMaxUsage) {
            Long hasOverMaxUsage = getZkStorageNode(zkOverMaxUsagePath);
            Long lastOverTime = getZkStorageNode(zkDurationTimePath);
            if (hasOverMaxUsage == null || lastOverTime == null || hasOverMaxUsage == 0) {
                setZkStorageNode(zkOverMaxUsagePath, 1L);
                setZkStorageNode(zkDurationTimePath, DateUtil.currentSeconds());
                JobUtil.warnLog("The log storage usage has first over the max usage limit.");
                return false;
            }
            long durationTime = (DateUtil.currentSeconds() - lastOverTime) / 60;
            if (hasOverMaxUsage == 1 && durationTime >= durationMinutes) {
                JobUtil.infoLog("The deletion task has been out of limit for {} minutes", durationMinutes);
                setZkStorageNode(zkOverMaxUsagePath, 0L);
                setZkStorageNode(zkDurationTimePath, DateUtil.currentSeconds());
                return true;
            } else {
                JobUtil.infoLog("The current overrun time {} is less than {} minutes", durationTime, durationMinutes);
            }
        }
        return false;
    }

    private boolean checkStorageInterval(String zkClearTimePath, int minIntervalMinutes) {
        long now = DateUtil.currentSeconds();
        Long lastStorage = getZkStorageNode(zkClearTimePath);
        if (lastStorage != null && now - lastStorage < minIntervalMinutes * 60) {
            JobUtil.infoLog("The deletion task is running");
            return false;
        }
        return true;
    }

    private double getUsage(Long totalSize, Long usedSize) {
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

        List<SysStorageEvent> newResultList = new ArrayList<>();
        long timestamp = DateUtils.convertStringToTimestamp(DateUtils.getDateOfYesterday("yyyyMMdd"), "yyyyMMdd");
        for (SysStorageEvent sysStorageEvent : jobResult.getData()) {
            Long dayBytes = sysStorageEvent.getBytes();
            long minBytes = dayBytes / num;
            for (long i = 0; i < num; i++) {
                SysStorageEvent newStorageEvent = new SysStorageEvent();
                BeanUtil.copyProperties(sysStorageEvent,newStorageEvent);
                newStorageEvent.setBytes(minBytes);

                timestamp = timestamp + interval * 60;
                newStorageEvent.setGeneratedTime(timestamp);
                newResultList.add(newStorageEvent);
            }
        }
        jobResult.setData(newResultList);
        JobUtil.infoLog("Set storage data ... ");
    }

    private JobResult<SysStorageEvent> getStorageQuota(JDBCParam jdbcParam, boolean containIncrease, Map<String, String> dataCenterMap, String logType, String zkNode) {
        List<SysStorageEvent> sysStorageEvents = Lists.newArrayList();
        JobResult<SysStorageEvent> jobResult = new JobResult<>();
        int failCount = 0;
        long generatedTime = DateUtil.currentSeconds();
        Long sinceTime = getZkStorageNode(zkNode);
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

    private List<DailyStorage> getHistoryStorageDaily(JDBCParam jdbcParam, String logType, String dataCenterName, String dataCenterHost) throws Exception {
        List<DailyStorage> dailyStorageList = new ArrayList<>();
        switch (logType) {
            case Constant.TRAFFIC_LOGS:
                dailyStorageList = getClickhouseHistoryDaily(dataCenterHost);
                break;
            case Constant.METRICS:
                dailyStorageList = getDruidHistoryDaily(dataCenterHost);
                break;
            case Constant.FILES:
                dailyStorageList = getMysqlHistoryStorageDaily(jdbcParam, logType, dataCenterName);
                break;
            default:
                break;
        }
        return dailyStorageList;
    }

    private Long getIncreaseSize(JDBCParam jdbcParam, Long usedSize, String datacenter, String logType) throws Exception {
        final String lastUsedSizeSql = "SELECT used_size FROM " + jdbcParam.getTable() + " WHERE log_type = '" + logType + "' and data_center = '" + datacenter
                + "' and generated_time >=  UNIX_TIMESTAMP() - " + Constant.ONE_WEEK + " and generated_time <=  UNIX_TIMESTAMP() - " + Constant.ONE_DAY + " ORDER BY generated_time DESC LIMIT 1;";
        JobUtil.infoLog(lastUsedSizeSql);

        try (Connection conn = DBUtils.getDBConn(jdbcParam)) {
            Number lastUsedSize = SqlExecutor.query(conn, lastUsedSizeSql, new NumberHandler());
            if (lastUsedSize == null || lastUsedSize.longValue() < 0 || usedSize - lastUsedSize.longValue() <= 0) {
                return 0L;
            }
            JobUtil.infoLog("Get {} log last used size is {} bytes {}", logType, lastUsedSize, usedSize - lastUsedSize.longValue());
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
        storageSizeInfo.setUsage(getUsage(totalSize, usedSize));
        JobUtil.infoLog(storageSizeInfo.toString());
        return storageSizeInfo;
    }

    public StorageSizeInfo getClickhouseStorageInfoByUnreserved(String dataCenterHost) {
        Map<String, Object> ckParamMap = storgeConfig.getCkSource();
        // 1. 使用率
        ckParamMap.put("query", clickhouseUsageSql);
        String result = HttpClientUtils.httpGet(HttpClientUtils.getUrlWithParams(UrlUtil.getUrl(dataCenterHost), ckParamMap));
        if ("-1".equals(result)) {
            throw new BusinessException("Get clickhouse http fail -1");
        }
        Double usage = Double.valueOf(result.trim());
        usage = usage > 1D ? 1D : usage;
        JobUtil.infoLog("clickhouse usage is : {}", usage);
        // 1. 总计
        ckParamMap.put("query", clickhouseMaxSizeSql);
        Long totalSize = queryClickhouse(dataCenterHost, ckParamMap);
        //2. 已使用
        ckParamMap.put("query", clickhouseUsedSizeSql);
        Long usedSize = queryClickhouse(dataCenterHost, ckParamMap);
        usedSize = usedSize > totalSize ? totalSize : usedSize;

        StorageSizeInfo storageSizeInfo = new StorageSizeInfo();
        storageSizeInfo.setTotalSize(totalSize);
        storageSizeInfo.setUsedSize(usedSize);
        storageSizeInfo.setUsage(usage);
        JobUtil.infoLog(storageSizeInfo.toString());

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
        storageSizeInfo.setUsage(getUsage(totalSize, usedSize));
        JobUtil.infoLog(storageSizeInfo.toString());
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
        storageSizeInfo.setUsage(getUsage(totalSize, usedSize));
        JobUtil.infoLog(storageSizeInfo.toString());
        return storageSizeInfo;
    }

    private List<DailyStorage> getClickhouseHistoryDaily(String dataCenterHost) throws Exception {
        StorageQuota storageQuota = getStorageQuota(Constant.TRAFFIC_LOGS);

        Map<String, Object> ckParamMap = storgeConfig.getCkSource();

        ckParamMap.put("query", StrUtil.format(clickhouseDailyUsedSizeSql, storgeConfig.getInSql("table", storageQuota.getSource_list())));
        String result = HttpClientUtils.httpGet(HttpClientUtils.getUrlWithParams(UrlUtil.getUrl(dataCenterHost), ckParamMap));
        if ("-1".equals(result)) {
            throw new BusinessException("Get clickhouse http fail -1");
        }
        Map resultMap = JSONUtil.toBean(result, Map.class);
        List<Map> dataList = (List) resultMap.get("data");
        List<DailyStorage> dailyStorageList = Lists.newArrayList();
        for (Map map : dataList) {
            DailyStorage dailyStorage = new DailyStorage();
            dailyStorage.setDate(DateUtils.convertStringToTimestamp(String.valueOf(map.get("date")), "yyyyMMdd"));
            dailyStorage.setBytes(TypeUtils.castToLong(map.get("bytes")));
            dailyStorageList.add(dailyStorage);
        }

        return dailyStorageList;
    }

    private List<DailyStorage> getDruidHistoryDaily(String dataCenterHost) throws Exception {
        StorageQuota storageQuota = getStorageQuota(Constant.METRICS);

        String result = HttpClientUtils.httpPost(UrlUtil.getUrl(dataCenterHost, Constant.DRUID_URL), StrUtil.format(druidDailyUsedSizeSql, storgeConfig.getInSql("datasource", storageQuota.getSource_list())));
        if ("-1".equals(result)) {
            throw new BusinessException("Get druid http fail -1");
        }
        List<Map> dataList = JSONUtil.toList(result, Map.class);
        List<DailyStorage> dailyStorageList = Lists.newArrayList();
        for (Map map : dataList) {
            DailyStorage dailyStorage = new DailyStorage();
            dailyStorage.setDate(DateUtils.convertStringToTimestamp(String.valueOf(map.get("date")), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
            dailyStorage.setBytes(TypeUtils.castToLong(map.get("bytes")));
            dailyStorageList.add(dailyStorage);
        }
        return dailyStorageList;
    }

    /**
     * @param node
     * @return 存在zookeeper 节点/storage/worker/+node
     */
    private Long getZkStorageNode(String node) {
        try {
            ZookeeperUtils zk = new ZookeeperUtils();
            String nodeData = zk.getNodeData(Constant.ZOOKEEPER_STORAGE_PATH + node, zookeeperServer);
            if (ObjectUtils.isEmpty(nodeData)) {
                return null;
            }
            Long lastStorage = Long.valueOf(nodeData);
            logger.info("query storage success,{}", lastStorage);
            return Long.valueOf(nodeData);
        } catch (Exception e) {
            throw new BusinessException("query standard time last_storage fail ," + JobUtil.getErrorMsg(e));
        }
    }

    private void setZkStorageNode(String path, Long data) {
        ZookeeperUtils zk = new ZookeeperUtils();
        zk.modifyNode(Constant.ZOOKEEPER_STORAGE_PATH + path, String.valueOf(data), zookeeperServer);
    }

    private Long getDeleteDate(List<DailyStorage> dailyStorageList, Long totalSize, Double maxUsage, String zkPath, String dataCenterName) {

        Collections.sort(dailyStorageList, Comparator.comparingLong(DailyStorage::getDate).reversed());

        long sumBytes = 0;
        Long deleteDate = null;
        for (DailyStorage dailyStorage : dailyStorageList) {
            sumBytes += dailyStorage.getBytes();

            double currUsage = getUsage(totalSize, sumBytes);
            JobUtil.infoLog("The date {} sum(bytes)/totalSize is {}", dailyStorage.getDate(), currUsage);
            if (currUsage >= maxUsage) {
                JobUtil.infoLog("The last date to reach or exceed is {}", dailyStorage.getDate());
                //记录到达maxUsage的日期
                deleteDate = dailyStorage.getDate() + Constant.ONE_DAY;
                break;
            }

        }
        if (deleteDate == null) {
            //获取不到存储配额信息时，使用默认值
            Long dcLastStorageTime = getZkStorageNode(StrUtil.join("/", zkPath, dataCenterName));
            if (dcLastStorageTime != null) {
                deleteDate = dcLastStorageTime + Constant.ONE_DAY;
            } else {
                Long lastStorage = getZkStorageNode(zkPath);
                deleteDate = lastStorage == null ? Constant.ONE_MONTH : lastStorage + Constant.ONE_DAY;
            }

        }
        long currDate = DateUtil.parseDate(DateUtil.today()).getTime() / 1000;
        deleteDate = deleteDate > currDate ? currDate : deleteDate;
        JobUtil.infoLog("The delete date is {}", deleteDate);
        return deleteDate;
    }

    public StorageQuota getStorageQuota(String logType) throws Exception {

        String responseString = HttpClientUtils.httpGet(UrlUtil.getUrl(storgeConfig.getQgwServer(), Constant.QGW_STORAGE_QUOTA_URL), Constant.QGW_HEADER);
        JSONObject paramsObject = JSONUtil.parseObj(responseString);

        JSONArray dataJsonArray = paramsObject.getJSONArray("data");
        List<StorageQuota> storageQuotaList = JSONUtil.toList(dataJsonArray, StorageQuota.class);
        StorageQuota storageQuota = storageQuotaList.stream()
                .filter(s -> logType.equals(s.getType()))
                .findFirst().orElseThrow(() -> new BusinessException("query gateway storage quota error "));
        JobUtil.infoLog(storageQuota.toString());
        return storageQuota;
    }

    /**
     * @param jdbcParam
     * @param logType
     * @param dataCenterName
     * @return
     * @throws SQLException
     * @Description 查询数据，获取历史每日存储配额,区分分数据中心日志类型
     */
    private List<DailyStorage> getMysqlHistoryStorageDaily(JDBCParam jdbcParam, String logType, String dataCenterName) throws SQLException {
        try (Connection conn = DBUtils.getDBConn(jdbcParam)) {
            List<DailyStorage> dailyStorageList = SqlExecutor.query(conn, StrUtil.format(mysqlDailyUsedSizeSql, jdbcParam.getDatabase(),
                    jdbcParam.getTable(), logType, dataCenterName), new BeanListHandler<>(DailyStorage.class));
            JobUtil.infoLog(dailyStorageList.toString());
            return dailyStorageList;
        } catch (Exception e) {
            throw e;
        }
    }

    @Data
    class StorageSizeInfo {
        private Long totalSize;
        private Long usedSize;
        private Double usage;
    }

    @Data
    class DailyStorage {
        private long bytes;
        private Long date;
    }

}
