package com.mesalab.executor.service;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.db.handler.NumberHandler;
import cn.hutool.db.sql.SqlExecutor;
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
import org.apache.groovy.util.Maps;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import javax.annotation.Resource;
import javax.management.timer.Timer;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

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
    final String clickhouseMaxSizeSql = StrUtil.format("SELECT SUM(`total_space`) FROM {};", storgeConfig.getSystemDisks());
    final String clickhouseUsedSizeSql = StrUtil.format("SELECT SUM(`bytes_on_disk`) FROM {} WHERE database = '{}' ;", storgeConfig.getSystemPartsCluster(), storgeConfig.getTrafficDatasource());

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
                        jobResult = getClickhouseStorageInfo(jdbcParam, containBytes);
                        break;
                    case Constant.METRICS:
                        jobResult = getDruidStorageInfo(jdbcParam, containBytes);
                        break;
                    case Constant.FILES:
                        jobResult = getHosStorageInfo(jdbcParam, containBytes);
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
                    failCount+=  deleteClickhouseOldLog(maxUsage, minIntervalMinutes);
                    break;
                case Constant.METRICS:
                    failCount+=  deleteDruidOldLog(maxUsage, minIntervalMinutes);
                    break;
                case Constant.FILES:
                    failCount+=  deleteHosOldLog(maxUsage, minIntervalMinutes);
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

    public int deleteClickhouseOldLog(int maxUsage, int minIntervalMinutes) {
        int failCount = 0;
        for (Map.Entry<String, String> datacenterMap : storgeConfig.getTrafficDataCenter().entrySet()) {
            try {
                String dataCenterHost = datacenterMap.getValue();
                String dataCenterName = datacenterMap.getKey();

                String zkClearTimePath = StrUtil.join("/", Constant.ZK_TRAFFIC_LOGS, dataCenterName, Constant.ZOOKEEPER_STORAGE_CLEAR_TIME);

                if (!checkStorageInterval(zkClearTimePath, minIntervalMinutes)) {
                    continue;
                }
                double clickhouseUsage = getClickhouseUsage(dataCenterHost, dataCenterName);
                if (maxUsage <= clickhouseUsage * 100d) {
                    // 1.获取删除天数 /datacenter maxDays = now - data_center_last_storage - 1
                    // 2.空则使用 maxDays = now - last_storage - 1
                    // 3.调用删除接口 参数maxDays, 记录日志
                    // 4.修改 datacenter时间
                    String zkDcLastStoragePath = StrUtil.join("/", Constant.ZK_TRAFFIC_LOGS, dataCenterName);
                    Long lastStorageTime = getLastStorage(zkDcLastStoragePath);
                    if (lastStorageTime == null) {
                        lastStorageTime = getLastStorage(Constant.ZK_TRAFFIC_LOGS);
                    }
                    // 删除1天
                    long storageTime = lastStorageTime - Timer.ONE_DAY;
                    Long maxDays = DateUtil.currentSeconds() - storageTime;

                    ReturnT<String> deleteResult = logStorageQuotaJob.deleteTrafficDataByCluster(Maps.of(Constant.MAX_DAYS, maxDays).toString());
                    if (deleteResult.getCode() != 200) {
                        failCount++;
                        JobUtil.errorLog("{} clickhouse log delete fail {}", dataCenterName, deleteResult.getMsg());
                        continue;
                    }
                    setLastStorage(zkDcLastStoragePath, storageTime);
                    JobUtil.infoLog("{} clickhouse log delete success {}");
                }
            } catch (Exception e) {
                failCount++;
                JobUtil.errorLog("clickhouse delete log error {}", e.getMessage());
            }
        }

        return failCount;
    }

    public int deleteDruidOldLog(int maxUsage, int minIntervalMinutes) {
        int failCount = 0;
        for (Map.Entry<String, String> datacenterMap : storgeConfig.getAnalyticDataCenter().entrySet()) {
            try {
                String dataCenterHost = datacenterMap.getValue();
                String dataCenterName = datacenterMap.getKey();

                String zkClearTimePath = StrUtil.join("/", Constant.ZK_REPORT_AND_METRICS, dataCenterName, Constant.ZOOKEEPER_STORAGE_CLEAR_TIME);

                if (!checkStorageInterval(zkClearTimePath, minIntervalMinutes)) {
                    continue;
                }
                double clickhouseUsage = getClickhouseUsage(dataCenterHost, dataCenterName);
                if (maxUsage <= clickhouseUsage * 100d) {
                    // 1.获取删除天数 /datacenter maxDays = now - data_center_last_storage - 1
                    // 2.空则使用 maxDays = now - last_storage - 1
                    // 3.调用删除接口 参数maxDays, 记录日志
                    // 4.修改 datacenter时间
                    String zkDcLastStoragePath = StrUtil.join("/", Constant.ZK_REPORT_AND_METRICS, dataCenterName);
                    Long lastStorageTime = getLastStorage(zkDcLastStoragePath);
                    if (lastStorageTime == null) {
                        lastStorageTime = getLastStorage(Constant.ZK_REPORT_AND_METRICS);
                    }
                    // 删除1天
                    long storageTime = lastStorageTime - Timer.ONE_DAY;
                    Long maxDays = DateUtil.currentSeconds() - storageTime;

                    ReturnT<String> deleteResult = logStorageQuotaJob.deleteReportAndMetricsData(Maps.of(Constant.MAX_DAYS, maxDays).toString());
                    if (deleteResult.getCode() != 200) {
                        failCount++;
                        JobUtil.errorLog("{} druid log delete fail {}", dataCenterName, deleteResult.getMsg());
                        continue;
                    }
                    setLastStorage(zkDcLastStoragePath, storageTime);
                    JobUtil.infoLog("{} druid log delete success {}");
                }
            } catch (Exception e) {
                failCount++;
                JobUtil.errorLog("druid delete log error {}", e.getMessage());
            }
        }

        return failCount;
    }

    public int deleteHosOldLog(int maxUsage, int minIntervalMinutes) {
        int failCount = 0;
        for (Map.Entry<String, String> datacenterMap : storgeConfig.getFilesDataCenter().entrySet()) {
            try {
                String dataCenterHost = datacenterMap.getValue();
                String dataCenterName = datacenterMap.getKey();

                String zkClearTimePath = StrUtil.join("/", Constant.FILES, dataCenterName, Constant.ZOOKEEPER_STORAGE_CLEAR_TIME);

                if (!checkStorageInterval(zkClearTimePath, minIntervalMinutes)) {
                    continue;
                }
                double clickhouseUsage = getClickhouseUsage(dataCenterHost, dataCenterName);
                if (maxUsage <= clickhouseUsage * 100d) {
                    // 1.获取删除天数 /datacenter maxDays = now - data_center_last_storage - 1
                    // 2.空则使用 maxDays = now - last_storage - 1
                    // 3.调用删除接口 参数maxDays, 记录日志
                    // 4.修改 datacenter时间
                    String zkDcLastStoragePath = StrUtil.join("/", Constant.FILES, dataCenterName);
                    Long lastStorageTime = getLastStorage(zkDcLastStoragePath);
                    if (lastStorageTime == null) {
                        lastStorageTime = getLastStorage(Constant.FILES);
                    }
                    // 删除1天
                    long storageTime = lastStorageTime - Timer.ONE_DAY;
                    Long maxDays = DateUtil.currentSeconds() - storageTime;

                    ReturnT<String> deleteResult = logStorageQuotaJob.deleteFiles(Maps.of(Constant.MAX_DAYS, maxDays).toString());
                    if (deleteResult.getCode() != 200) {
                        failCount++;
                        JobUtil.errorLog("{} hos log delete fail {}", dataCenterName, deleteResult.getMsg());
                        continue;
                    }
                    setLastStorage(zkDcLastStoragePath, storageTime);
                    JobUtil.infoLog("{} hos log delete success {}");
                }
            } catch (Exception e) {
                failCount++;
                JobUtil.errorLog("hos delete log error {}", e.getMessage());
            }
        }

        return failCount;
    }

    private boolean checkStorageInterval(String zkClearTimePath, int minIntervalMinutes) {
        long now = DateUtil.currentSeconds();
        Long lastStorage = getLastStorage(zkClearTimePath);
        if (lastStorage != null && now - lastStorage <= minIntervalMinutes * 60) {
            JobUtil.infoLog("The deletion task is being executed");
            return false;
        }
        return true;
    }

    private double getClickhouseUsage(String dataCenterHost, String dataCenterName) {
        Map<String, Object> ckParamMap = storgeConfig.getCkSource();
        // 1. 总计
        ckParamMap.put("query", clickhouseMaxSizeSql);
        Long totalSize = queryClickhouse(dataCenterHost, ckParamMap);
        //2. 已使用
        ckParamMap.put("query", clickhouseUsedSizeSql);
        Long usedSize = queryClickhouse(dataCenterHost, ckParamMap);
        double usage = usedSize / (double) totalSize;
        usage = Double.valueOf(String.format("%.4f", usage));
        JobUtil.infoLog("clickhouse total size {}, used size {}, usage {}",totalSize, usedSize, usage);
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

    private JobResult<SysStorageEvent> getClickhouseStorageInfo(JDBCParam jdbcParam, boolean containIncrease) throws Exception {

        List<SysStorageEvent> sysStorageEvents = Lists.newArrayList();
        JobResult<SysStorageEvent> jobResult = new JobResult();
        int failCount = 0;
        long generatedTime = DateUtil.currentSeconds();
        Long sinceTime = getLastStorage(Constant.ZK_TRAFFIC_LOGS);
        for (Map.Entry<String, String> datacenterMap : storgeConfig.getTrafficDataCenter().entrySet()) {
            try {
                String datacenterHost = datacenterMap.getValue();
                String datacenterName = datacenterMap.getKey();
                Map<String, Object> ckParamMap = storgeConfig.getCkSource();
                // 1. 总计
                ckParamMap.put("query", clickhouseMaxSizeSql);
                Long totalSize = queryClickhouse(datacenterHost, ckParamMap);
                //2. 已使用
                ckParamMap.put("query", clickhouseUsedSizeSql);
                Long usedSize = queryClickhouse(datacenterHost, ckParamMap);

                //3. 增量
                Long bytes = containIncrease ? 0L : getIncreaseSize(jdbcParam, usedSize, datacenterMap.getKey(), Constant.TRAFFIC_LOGS);
                SysStorageEvent storageEvent = SysStorageEvent.builder()
                        .logType(Constant.TRAFFIC_LOGS)
                        .dataCenter(datacenterName)
                        .generatedTime(generatedTime)
                        .totalAllocatedSize(totalSize)
                        .usedSize(usedSize)
                        .bytes(bytes)
                        .sinceTime(sinceTime)
                        .build();

                sysStorageEvents.add(storageEvent);
                JobUtil.infoLog("Get clickhouse storage info {}:", storageEvent.toString());
            } catch (Exception e) {
                failCount++;
                JobUtil.errorLog("Get clickhouse storage error : datacenter {}, message {}", datacenterMap.getKey(), e.getMessage());
            }
        }
        jobResult.setFailCount(failCount);
        jobResult.setData(sysStorageEvents);
        return jobResult;
    }

    private JobResult<SysStorageEvent> getDruidStorageInfo(JDBCParam jdbcParam, boolean containIncrease) throws Exception {

        final String maxSizeSql = "{\"query\":\"SELECT SUM(max_size) AS max_size FROM sys.servers WHERE server_type = 'historical'\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"csv\"}";
        final String usedSizeSql = "{\"query\":\"SELECT SUM(size) AS used_size FROM sys.segments WHERE datasource NOT LIKE '%hot%' and is_published = 1 and is_overshadowed = 0\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"csv\"}";

        List<SysStorageEvent> sysStorageEvents = Lists.newArrayList();
        JobResult<SysStorageEvent> jobResult = new JobResult();
        int failCount = 0;
        long generatedTime = DateUtil.currentSeconds();
        Long sinceTime = getLastStorage(Constant.ZK_REPORT_AND_METRICS);
        for (Map.Entry<String, String> datacenterMap : storgeConfig.getAnalyticDataCenter().entrySet()) {
            try {
                String datacenterHost = datacenterMap.getValue();
                String datacenterName = datacenterMap.getKey();
                // 1. 总计
                Long totalSize = queryDruid(datacenterHost, maxSizeSql);
                //2. 已使用
                Long usedSize = queryDruid(datacenterHost, usedSizeSql);

                //3. 增量
                Long bytes = containIncrease ? 0L : getIncreaseSize(jdbcParam, usedSize, datacenterMap.getKey(), Constant.METRICS);

                SysStorageEvent storageEvent = SysStorageEvent.builder()
                        .logType(Constant.METRICS)
                        .dataCenter(datacenterName)
                        .generatedTime(generatedTime)
                        .totalAllocatedSize(totalSize)
                        .usedSize(usedSize)
                        .bytes(bytes)
                        .sinceTime(sinceTime)
                        .build();

                sysStorageEvents.add(storageEvent);
                JobUtil.infoLog("Get druid storage info {}:", storageEvent.toString());
            } catch (Exception e) {
                failCount++;
                JobUtil.errorLog("Get druid storage error : datacenter {}, message {}", datacenterMap.getKey(), e.getMessage());
            }
        }
        jobResult.setFailCount(failCount);
        jobResult.setData(sysStorageEvents);
        return jobResult;
    }

    private JobResult<SysStorageEvent> getHosStorageInfo(JDBCParam jdbcParam, boolean containIncrease) throws Exception {

        List<SysStorageEvent> sysStorageEvents = Lists.newArrayList();
        JobResult<SysStorageEvent> jobResult = new JobResult();
        int failCount = 0;
        Long generatedTime = DateUtil.currentSeconds();
        Long sinceTime = getLastStorage(Constant.FILES);
        for (Map.Entry<String, String> datacenterMap : storgeConfig.getFilesDataCenter().entrySet()) {
            try {
                String datacenterHost = datacenterMap.getValue();
                String datacenterName = datacenterMap.getKey();

                HosSpace hosSpace = getHosSpace(datacenterHost, datacenterName);
                Long totalSize = hosSpace.getHosCapacity();
                Long usedSize = hosSpace.getHosUsed();
                if (totalSize == -1 || usedSize == -1) {
                    throw new BusinessException("hos server error : " + datacenterName);
                }

                //3. 增量
                Long bytes = containIncrease ? 0L : getIncreaseSize(jdbcParam, usedSize, datacenterMap.getKey(), Constant.FILES);
                SysStorageEvent storageEvent = SysStorageEvent.builder()
                        .logType(Constant.FILES)
                        .dataCenter(datacenterName)
                        .generatedTime(generatedTime)
                        .totalAllocatedSize(totalSize)
                        .usedSize(usedSize)
                        .bytes(bytes)
                        .sinceTime(sinceTime)
                        .build();

                sysStorageEvents.add(storageEvent);
                JobUtil.infoLog("Get hos storage info {}:", storageEvent.toString());
            } catch (Exception e) {
                failCount++;
                JobUtil.errorLog("Get hos storage error : datacenter {}, message {}", datacenterMap.getKey(), e.getMessage());
            }
        }
        jobResult.setFailCount(failCount);
        jobResult.setData(sysStorageEvents);
        return jobResult;
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

    public HosSpace getHosSpace(String datacenterHost, String datacenterName) {
        final String fileStoragePath = "/admin/diskspace";
        Header[] headers = {new BasicHeader(Constant.TOKEN, storgeConfig.getFilesToken()),
                new BasicHeader(HttpHeaders.CONTENT_TYPE, Constant.TEXT_XML)};
        String result = HttpClientUtils.httpGet(UrlUtil.getUrl(datacenterHost).concat(fileStoragePath), headers);

        if ("-1".equals(result)) {
            throw new BusinessException("Get hos http request fail -1 , server: " + datacenterName);
        }
        HosSpace hosSpace = XmlUtil.converXmlToBean(HosSpace.class, result);
        return hosSpace;
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
        zk.modifyNode(path, String.valueOf(lastStorage), zookeeperServer);
    }

}
