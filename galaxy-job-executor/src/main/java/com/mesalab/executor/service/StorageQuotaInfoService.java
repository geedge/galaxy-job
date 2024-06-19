package com.mesalab.executor.service;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.db.handler.NumberHandler;
import cn.hutool.db.sql.SqlExecutor;
import cn.hutool.log.Log;
import com.google.common.collect.Lists;
import com.mesalab.executor.core.config.StorgeConfig;
import com.mesalab.executor.core.utils.*;
import com.mesalab.executor.exception.BusinessException;
import com.mesalab.executor.pojo.HosSpace;
import com.mesalab.executor.pojo.JDBCParam;
import com.mesalab.executor.pojo.SysStorageEvent;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

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
public class StorageQuotaInfoService {

    private StorgeConfig storgeConfig = (StorgeConfig) SpringContextUtil.getBean("storgeConfig");
    private Log logger = Log.get();

    //查询历史数据时允许的最大间隔60 MINUTE
    private static final int STORAGE_LIMITATION = 120 * 60;

    @Value("${zookeeper.server}")
    private String zookeeperServer;


    public int getAndSaveStorageQuotaInfo(String[] logTypes, JDBCParam jdbcParam) {

        int failCount = 0;
        for (String logType : logTypes) {
            try {
                switch (logType) {
                    case Constant.TRAFFIC_LOGS:
                        failCount += getAndSaveClickhouseStorageInfo(jdbcParam);
                        break;
                    case Constant.METRICS:
                        failCount += getAndSaveDruidStorageInfo(jdbcParam);
                        break;
                    case Constant.FILES:
                        failCount += getAndSaveHosStorageInfo(jdbcParam);
                        break;
                    default:
                        break;
                }

            } catch (Exception e) {
                failCount++;
                JobUtil.errorLog(e.getMessage());
            }
        }
        return failCount;
    }

    private int getAndSaveClickhouseStorageInfo(JDBCParam jdbcParam) throws Exception {

        final String maxSizeSql = StrUtil.format("SELECT SUM(`total_space`) FROM {};", storgeConfig.getSystemDisks());
        final String usedSizeSql = StrUtil.format("SELECT SUM(`bytes_on_disk`) FROM {} WHERE database = '{}' ;", storgeConfig.getSystemPartsCluster(), storgeConfig.getTrafficDatasource());

        long generatedTime = DateUtil.currentSeconds();
        Long sinceTime = getLastStorage(Constant.ZK_TRAFFIC_LOGS);
        List<SysStorageEvent> sysStorageEvents = Lists.newArrayList();
        int failCount = 0;
        for (Map.Entry<String, String> datacenterMap : storgeConfig.getTrafficDataCenter().entrySet()) {
            try {
                String datacenterHost = datacenterMap.getValue();
                String datacenterName = datacenterMap.getKey();
                Map<String, Object> ckParamMap = storgeConfig.getCkSource();
                // 1. 总计
                ckParamMap.put("query", maxSizeSql);
                String totalResult = HttpClientUtils.httpGet(HttpClientUtils.getUrlWithParams(UrlUtil.getUrl(datacenterHost), ckParamMap));
                //2. 已使用
                ckParamMap.put("query", usedSizeSql);
                String usedResult = HttpClientUtils.httpGet(HttpClientUtils.getUrlWithParams(UrlUtil.getUrl(datacenterHost), ckParamMap));

                if ("-1".equals(totalResult) || "-1".equals(usedResult)) {
                    throw new BusinessException("Get clickhouse http fail -1");
                }

                Long totalSize = Long.valueOf(totalResult.trim());
                Long usedSize = Long.valueOf(usedResult.trim());

                //3. 增量
                Long increaseSize = getIncreaseSize(jdbcParam, usedSize, datacenterMap.getKey(), Constant.TRAFFIC_LOGS);
                SysStorageEvent storageEvent = SysStorageEvent.builder()
                        .logType(Constant.TRAFFIC_LOGS)
                        .dataCenter(datacenterName)
                        .generatedTime(generatedTime)
                        .totalAllocatedSize(totalSize)
                        .usedSize(usedSize)
                        .bytes(increaseSize)
                        .sinceTime(sinceTime)
                        .build();

                sysStorageEvents.add(storageEvent);
                JobUtil.infoLog("Get clickhouse storage info : datacenter {}, max_size {}, used_size {}, bytes {}.",
                        datacenterName, totalSize, usedSize, increaseSize);
            } catch (Exception e) {
                failCount++;
                JobUtil.errorLog("Get clickhouse storage error : datacenter {}, message {}", datacenterMap.getKey(), e.getMessage());
            }
        }
        DBUtils.save(sysStorageEvents, jdbcParam);
        return failCount;
    }

    private int getAndSaveDruidStorageInfo(JDBCParam jdbcParam) throws Exception {

        final String maxSizeSql = "{\"query\":\"SELECT SUM(max_size) AS max_size FROM sys.servers WHERE server_type = 'historical'\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"csv\"}";
        final String usedSizeSql = "{\"query\":\"SELECT SUM(size) AS used_size FROM sys.segments WHERE datasource NOT LIKE '%hot%' and is_published = 1 and is_overshadowed = 0\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"csv\"}";
        final String druidPath = "/druid/v2/sql";

        long generatedTime = DateUtil.currentSeconds();
        Long sinceTime = getLastStorage(Constant.ZK_TRAFFIC_LOGS);
        List<SysStorageEvent> sysStorageEvents = Lists.newArrayList();
        int failCount = 0;
        for (Map.Entry<String, String> datacenterMap : storgeConfig.getAnalyticDataCenter().entrySet()) {
            try {
                String datacenterHost = datacenterMap.getValue();
                String datacenterName = datacenterMap.getKey();
                // 1. 总计
                String totalResult = HttpClientUtils.httpPost(UrlUtil.getUrl(datacenterHost).concat(druidPath), maxSizeSql);
                //2. 已使用
                String usedResult = HttpClientUtils.httpPost(UrlUtil.getUrl(datacenterHost).concat(druidPath), usedSizeSql);

                if ("-1".equals(totalResult) || "-1".equals(usedResult)) {
                    throw new BusinessException("Get druid http fail -1");
                }

                Long totalSize = Long.valueOf(totalResult.trim());
                Long usedSize = Long.valueOf(usedResult.trim());

                //3. 增量
                Long increaseSize = getIncreaseSize(jdbcParam, usedSize, datacenterMap.getKey(), Constant.METRICS);
                SysStorageEvent storageEvent = SysStorageEvent.builder()
                        .logType(Constant.METRICS)
                        .dataCenter(datacenterName)
                        .generatedTime(generatedTime)
                        .totalAllocatedSize(totalSize)
                        .usedSize(usedSize)
                        .bytes(increaseSize)
                        .sinceTime(sinceTime)
                        .build();

                sysStorageEvents.add(storageEvent);
                JobUtil.infoLog("Get druid storage info : datacenter {}, max_size {}, used_size {}, bytes {}.",
                        datacenterName, totalSize, usedSize, increaseSize);
            } catch (Exception e) {
                failCount++;
                JobUtil.errorLog("Get druid storage error : datacenter {}, message {}", datacenterMap.getKey(), e.getMessage());
            }
        }
        DBUtils.save(sysStorageEvents, jdbcParam);
        return failCount;
    }

    private int getAndSaveHosStorageInfo(JDBCParam jdbcParam) throws Exception {

        final String fileStoragePath = "/admin/diskspace";

        long generatedTime = DateUtil.currentSeconds();
        Long sinceTime = getLastStorage(Constant.FILES);
        List<SysStorageEvent> sysStorageEvents = Lists.newArrayList();
        int failCount = 0;
        for (Map.Entry<String, String> datacenterMap : storgeConfig.getFilesDataCenter().entrySet()) {
            try {
                String datacenterHost = datacenterMap.getValue();
                String datacenterName = datacenterMap.getKey();

                Header[] headers = {new BasicHeader(Constant.TOKEN, storgeConfig.getFilesToken()), new BasicHeader(HttpHeaders.CONTENT_TYPE, Constant.TEXT_XML)};
                String result = HttpClientUtils.httpGet(UrlUtil.getUrl(datacenterHost).concat(fileStoragePath), headers);

                if ("-1".equals(result) || "-1".equals(result)) {
                    throw new BusinessException("Get hos http request fail -1 , server: " + datacenterName);
                }
                HosSpace hosSpace = XmlUtil.converXmlToBean(HosSpace.class, result);
                Long totalSize = hosSpace.getHosCapacity();
                Long usedSize = hosSpace.getHosUsed();
                if (totalSize == -1 || usedSize == -1) {
                    throw new BusinessException("hos server error server: " + datacenterName);
                }

                //3. 增量
                Long increaseSize = getIncreaseSize(jdbcParam, usedSize, datacenterMap.getKey(), Constant.FILES);
                SysStorageEvent storageEvent = SysStorageEvent.builder()
                        .logType(Constant.FILES)
                        .dataCenter(datacenterName)
                        .generatedTime(generatedTime)
                        .totalAllocatedSize(totalSize)
                        .usedSize(usedSize)
                        .bytes(increaseSize)
                        .sinceTime(sinceTime)
                        .build();

                sysStorageEvents.add(storageEvent);
                JobUtil.infoLog("Get hos storage info : datacenter {}, max_size {}, used_size {}, bytes {}.",
                        datacenterName, totalSize, usedSize, increaseSize);
            } catch (Exception e) {
                failCount++;
                JobUtil.errorLog("Get hos storage error : datacenter {}, message {}", datacenterMap.getKey(), e.getMessage());
            }
        }
        DBUtils.save(sysStorageEvents, jdbcParam);
        return failCount;
    }

    private Long getIncreaseSize(JDBCParam jdbcParam, Long usedSize, String datacenter, String logType) throws Exception {
        final String lastUsedSizeSql = "SELECT used_size FROM " + jdbcParam.getTable() + " WHERE log_type = '" + logType + "' and data_center = '" + datacenter
                + "' and generated_time >=  UNIX_TIMESTAMP() - " + STORAGE_LIMITATION + " ORDER BY generated_time DESC LIMIT 1;";
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

    /**
     * @param node
     * @return 自定义的标准时间 last_storage,
     * 存在zookeeper 节点/storage/worker/+node
     */
    private Long getLastStorage(String node) {
        try {
            ZookeeperUtils zk = new ZookeeperUtils();
            String nodeData = zk.getNodeData(Constant.ZOOKEEPER_STORAGE_PATH + node, zookeeperServer);
            //不存在创建一个
            if (ObjectUtils.isEmpty(nodeData)) {
                Long lastTime = System.currentTimeMillis() / 1000;
                zk.modifyNode(Constant.ZOOKEEPER_STORAGE_PATH + node, String.valueOf(lastTime), zookeeperServer);
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


}
