package com.mesalab.executor.jobhandler;

import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import com.geedgenetworks.utils.DateUtils;
import com.geedgenetworks.utils.JsonMapper;
import com.geedgenetworks.utils.StringUtil;
import com.geedgenetworks.utils.TimeConstants;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.mesalab.executor.core.config.StorgeConfig;
import com.mesalab.executor.core.utils.*;
import com.mesalab.executor.exception.BusinessException;
import com.mesalab.executor.pojo.LifecycleConfiguration;
import com.mesalab.executor.pojo.StorageQuota;
import com.mesalab.executor.service.StorageQuotaService;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Component
public class LogStorageQuotaJob {

    private Log logger = Log.get();

    private static final String analyticUrl = "/druid/v2/sql";
    private static final String ck_cluster = "ck_cluster";
    private static final String ck_table_suffix = "_local";
    private static final String trafficPort = "8123";

    @Value("${zookeeper.server}")
    private String zookeeperServer;

    private StorgeConfig deletionConfig = (StorgeConfig) SpringContextUtil.getBean("storgeConfig");
    //*********************************************设置存储策略**********************************************************
    private final Header[] hosHeaders = {new BasicHeader(Constant.TOKEN, deletionConfig.getFilesToken()),
            new BasicHeader(HttpHeaders.CONTENT_TYPE, Constant.TEXT_XML)};

    @Resource
    StorageQuotaService storageQuotaService;

    /**
     * 设置流量日志存储策略储
     * clickhouse
     */
    @XxlJob("deleteTrafficDataJobHandler")
    public ReturnT<String> deleteTrafficData(String params) {
        try {
            Map<String, Object> ckParamsMap = deletionConfig.getCkSource();
            StorageQuota storageQuota = storageQuotaService.getStorageQuota(Constant.TRAFFIC_LOGS);
            Integer maxDays = getMaxDays(params, storageQuota.getMax_days());
            String dropPartSqlTemp = "ALTER TABLE " + deletionConfig.getTrafficDatasource() + ".{} DROP PARTITION {}";
            String queryPartSqlTemp = "SELECT DISTINCT(`partition`) from " + deletionConfig.getSystemParts() + " WHERE table = '{}' AND `partition` < '{}' FORMAT JSON;";

            String deleteMaxDate = DateUtils.getSomeDate(TimeConstants.YYYYMMDD, -maxDays);
            List<String> addressForCKs = getAddressForCK(UrlUtil.getUrl(deletionConfig.getTrafficServer()));
            int failCount = 0;
            // 指定服务器
            for (String endpoint : addressForCKs) {
                try {
                    JobUtil.infoLog("-----------------reset storage clickhouse ttl endpoint = {}  ------------------", endpoint);
                    String endpointUrl = UrlUtil.getUrl(endpoint);
                    for (String table : storageQuota.getSource_list()) {
                        String localTable = table + ck_table_suffix;
                        try {
                            //指定分区
                            List<String> partitions = getSystemDataForCK(endpointUrl, StrUtil.format(queryPartSqlTemp, localTable, deleteMaxDate));
                            for (String partition : partitions) {
                                try {
                                    ckParamsMap.put("query", StrUtil.format(dropPartSqlTemp, localTable, partition));
                                    HttpClientUtils.httpPost(HttpClientUtils.getUrlWithParams(endpointUrl, ckParamsMap), "");
                                } catch (BusinessException e) {
                                    failCount++;
                                    JobUtil.errorLog("clickhouse ttl error table = {}, partition = {}, fail message = {}", localTable, partition, JobUtil.getErrorMsg(e));
                                }
                            }
                            JobUtil.infoLog("reset clickhouse ttl table = {}, partition < {}, size = {}, fail count = {}", localTable, deleteMaxDate, partitions, failCount);
                        } catch (BusinessException e) {
                            failCount++;
                            JobUtil.errorLog("clickhouse ttl error table = {}, fail message = {}", localTable, JobUtil.getErrorMsg(e));
                        }
                    }
                } catch (BusinessException e) {
                    failCount++;
                    JobUtil.errorLog("clickhouse ttl error endpoint = {}, fail message = {}", endpoint, JobUtil.getErrorMsg(e));
                }
            }
            modifyLastStorage(Constant.ZK_TRAFFIC_LOGS, maxDays);
            if (failCount > 0) {
                throw new BusinessException("reset storage clickhouse ttl error ,failCount " + failCount);
            }

        } catch (Exception be) {
            JobUtil.errorLog(be.getMessage());
            return ReturnT.FAIL;
        }
        return ReturnT.SUCCESS;
    }

    /**
     * 设置流量日志存储策略储
     * clickhouse 数据库
     *
     * @param params {"maxDays":30}
     */
    @XxlJob("deleteTrafficDataJobHandler1")
    public ReturnT<String> deleteTrafficDataByCluster(String params) {
        try {

            Map<String, Object> deleteParamMap = deletionConfig.getCkSource();
            StorageQuota storageQuota = storageQuotaService.getStorageQuota(Constant.TRAFFIC_LOGS);

            String prefixDeleteSql = Joiner.on("").join("ALTER TABLE ", deletionConfig.getTrafficDatasource(), ".");
            String suffixDeleteSql = Joiner.on("").join(" ON CLUSTER ", ck_cluster, " DROP PARTITION ");
            Integer maxDays = getMaxDays(params, storageQuota.getMax_days());
            String deleteMaxDate = DateUtils.getSomeDate(TimeConstants.YYYYMMDD, -maxDays);
            String ckUrl = UrlUtil.getUrl(deletionConfig.getTrafficServer());

            int failCount = 0;
            // 设置一个获取sql的方法，把表List
            for (String table : storageQuota.getSource_list()) {
                String localTable = table + ck_table_suffix;
                try {
                    JobUtil.infoLog("-----------------reset storage clickhouse table = {}  ------------------", localTable);
                    List<String> partitionList = getSystemDataForCK(ckUrl, Joiner.on("").join("SELECT DISTINCT(`partition`) from " + deletionConfig.getSystemPartsCluster()
                            + " WHERE table = '", localTable, "' AND `partition` < '", deleteMaxDate, "' FORMAT JSON;"));
                    JobUtil.infoLog("reset storage days table {}, drop partition size {} ", localTable, partitionList.size());

                    partitionList.forEach(partition -> {
                        String deleteSql = Joiner.on("").join(prefixDeleteSql, localTable, suffixDeleteSql, "'", partition, "'");
                        deleteParamMap.put("query", deleteSql);
                        JobUtil.infoLog("reset storage days clickhouse sql:{}", deleteSql);

                        HttpClientUtils.httpPost(HttpClientUtils.getUrlWithParams(ckUrl, deleteParamMap), null);
                        JobUtil.infoLog("reset storage days table {} partition {} success", localTable, partition);
                    });
                } catch (BusinessException e) {
                    failCount++;
                    JobUtil.errorLog("clickhouse ttl error table = {}, fail message = {}", localTable, JobUtil.getErrorMsg(e));
                }
            }
            modifyLastStorage(Constant.ZK_TRAFFIC_LOGS, maxDays);
            if (failCount > 0) {
                throw new BusinessException("reset storage clickhouse ttl error, failCount " + failCount);
            }
            JobUtil.infoLog("reset clickhouse {} days success", maxDays);
        } catch (Exception be) {
            JobUtil.errorLog(be.getMessage());
            return ReturnT.FAIL;
        }

        return ReturnT.SUCCESS;
    }

    private Integer getMaxDays(String params, Integer maxDays) {
        if (ObjectUtils.isNotEmpty(params)) {
            JSONObject paramsJsonObj = JSONUtil.parseObj(params);
            Integer triggerMaxDays = paramsJsonObj.getInt(Constant.MAX_DAYS);
            maxDays = triggerMaxDays == null ? maxDays : triggerMaxDays;
        }
        return maxDays;
    }

    /**
     * Report and Metrics 日志存储策略
     * druid 数据库
     *
     * @param params{"maxdays":365}
     */
    @XxlJob("deleteReportAndMetricsDataJobHandler")
    public ReturnT<String> deleteReportAndMetricsData(String params) {

        try {
            StorageQuota storageQuota = storageQuotaService.getStorageQuota(Constant.METRICS);
            Integer maxDays = getMaxDays(params, storageQuota.getMax_days());
            deleteDruidTable(maxDays, storageQuota.getSource_list());
            modifyLastStorage(Constant.ZK_REPORT_AND_METRICS, maxDays);

        } catch (Exception be) {
            JobUtil.errorLog(be.getMessage());
            return ReturnT.FAIL;
        }
        return ReturnT.SUCCESS;
    }

    private void deleteDruidTable(Integer maxDays, List<String> tableList) {
        //1.修改规则
        String druidHost = deletionConfig.getAnalyticServer();
        String changeRulesUrl = UrlUtil.getUrl(druidHost, "/druid/coordinator/v1/rules/");
        String changeRulesParam = StrUtil.format("[{\"type\":\"loadByPeriod\",\"period\":\"P{}D\",\"tieredReplicants\":{\"_default_tier\":1}},{\"type\":\"dropForever\"}]", maxDays);
        //3 .物理删除
        String deleteUrl = UrlUtil.getUrl(druidHost, "/druid/indexer/v1/task");
        //起始时间/当前时间-maxDay
        String druidDataStartTime = getDruidDataStartTime();
        String deleteMaxDate = DateUtils.getSomeDate(TimeConstants.YYYY_MM_DD, -maxDays);
        String deleteParamTemp = String.join("","{\"type\":\"kill\",\"dataSource\":\"{}\",\"interval\":\"", druidDataStartTime, "/", deleteMaxDate, "\"}");
        if (druidDataStartTime.compareTo(deleteMaxDate) > 0) {
            JobUtil.infoLog("deletion range start date {} >  end date {}", druidDataStartTime, deleteMaxDate);
            return;
        }
        String unUseParam = String.join("","{\"interval\":\"", druidDataStartTime, "/", deleteMaxDate, "\"}");
        JobUtil.infoLog("reset storage days druid unused command = {}", unUseParam);

        int failCount = 0;
        for (String table : tableList) {
            try {
                JobUtil.infoLog("-----------------reset storage druid endpoint = {}, table = {} ------------------", druidHost, table);

                JobUtil.infoLog("reset storage days rules command = {}", changeRulesParam);
                HttpClientUtils.httpPost(changeRulesUrl + table, changeRulesParam);
                // 2. unuse
                String unUseMessage = HttpClientUtils.httpPost(UrlUtil.getUrl(druidHost, "/druid/coordinator/v1/datasources/", table, "/markUnused"), unUseParam);
                JobUtil.infoLog("reset storage days unused result message :{}", unUseMessage);

                String deleteDataCmd = StrUtil.format(deleteParamTemp, table);
                JobUtil.infoLog("reset storage days kill command = {}", deleteDataCmd);
                String deleteMessage = HttpClientUtils.httpPost(deleteUrl, deleteDataCmd);
                JobUtil.infoLog("reset storage days druid result message :{}", deleteMessage);
            } catch (BusinessException e) {
                failCount++;
                JobUtil.errorLog("reset storage druid ttl table = {}, fail message = {}", table, JobUtil.getErrorMsg(e));
            }
        }
        if (failCount > 0) {
            throw new BusinessException("reset storage druid ttl error, table fail count " + failCount);
        }
    }

    /**
     * 设置文件存储策略
     *
     * @param params{"maxdays":30}
     */
    @XxlJob("deleteFilesJobHandler")
    public ReturnT<String> deleteFiles(String params) {
        try {
            StorageQuota storageQuota = storageQuotaService.getStorageQuota(Constant.FILES);
            Integer maxDays = getMaxDays(params, storageQuota.getMax_days());
            List<String> hosHostList = StringUtil.isEmpty(deletionConfig.getFilesHosServer()) ? null : Splitter.on(",")
                    .trimResults()
                    .omitEmptyStrings()
                    .splitToList(deletionConfig.getFilesHosServer());

            int failCount = 0;
            for (String hosHost : hosHostList) {
                JobUtil.infoLog("-----------------reset storage hos endpoint = {} ------------------", hosHost);
                failCount += deleteHosData(hosHost, maxDays.toString(), storageQuota.getSource_list());
            }

            if (failCount > 0) {
                JobUtil.errorLog(" number of total endpoints " + hosHostList.size() + ", number of failed count " + failCount);
                return ReturnT.FAIL;
            }
        } catch (Exception be) {
            JobUtil.errorLog(be.getMessage());
            return ReturnT.FAIL;
        }
        return ReturnT.SUCCESS;
    }

    public int deleteHosData(String endpoint, String maxDays, List<String> bucketList) {
        int failCount = 0;
        try {
            String hosServer = UrlUtil.getUrl(endpoint);
            LifecycleConfiguration lifecycleConfig = new LifecycleConfiguration();

            lifecycleConfig.getRule().setExpiration(new LifecycleConfiguration.Expiration(maxDays));
            JobUtil.infoLog("reset storage files ttl endpoint = {}, bucketList = {} ", endpoint, bucketList);
            for (String bucketName : bucketList) {
                try {
                    HttpClientUtils.httpPut(StringUtils.join(hosServer, "/hos/", bucketName, "?lifecycle"), XmlUtil.converToXml(lifecycleConfig), hosHeaders);
                } catch (BusinessException e) {
                    failCount++;
                    JobUtil.errorLog("reset storage files ttl failCount = {}, endpoint = {}, bucket = {}, fail message = {}", failCount, endpoint, bucketName, JobUtil.getErrorMsg(e));
                }
            }
            modifyLastStorage(Constant.FILES, Integer.valueOf(maxDays));
        } catch (BusinessException e) {
            JobUtil.errorLog("reset storage files ttl endpoint = {}, fail message = {}", endpoint, JobUtil.getErrorMsg(e));
            failCount++;
        }
        return failCount;
    }
    //*********************************************清库**********************************************************

    /**
     * 清除所有流量数据,click hosue库
     *
     * @param params{"maxdays":365}
     */
    @XxlJob("deleteAllTrafficDataJobHandler")
    public ReturnT<String> deleteAllTrafficData(String params) {

        try {
            Map<String, Object> deleteParamMap = deletionConfig.getCkSource();
            StorageQuota storageQuota = storageQuotaService.getStorageQuota(Constant.TRAFFIC_LOGS);
            //清库命令参数
            String deleteSql = Joiner.on("").join("TRUNCATE TABLE IF EXISTS ", deletionConfig.getTrafficDatasource(), ".");
            int failCount = 0;
            try {
                String url = UrlUtil.getUrl(deletionConfig.getTrafficServer());
                JobUtil.infoLog("delete storage clickhouse endpoint = {}, tables = {}", url, storageQuota.getSource_list());
                for (String table : storageQuota.getSource_list()) {
                    try {
                        String deleteSqlCluster = String.join(" ", deleteSql, table, " ON CLUSTER ", ck_cluster);
                        deleteParamMap.put("query", deleteSqlCluster);

                        JobUtil.infoLog("delete clickhouse table = {}, sql:{}", table, deleteSqlCluster);
                        HttpClientUtils.httpPost(HttpClientUtils.getUrlWithParams(url, deleteParamMap), "");
                        JobUtil.infoLog("delete clickhouse table = {}", table);

                    } catch (BusinessException e) {
                        JobUtil.errorLog("delete storage clickhouse error endpoint = {}, table = {}, fail message = {}", url, table, JobUtil.getErrorMsg(e));
                        failCount++;
                    }
                }
            } catch (BusinessException e) {
                JobUtil.errorLog("delete storage clickhouse error ,  fail message = {}", JobUtil.getErrorMsg(e));
                failCount++;
            }

            modifyLastStorage(Constant.ZK_TRAFFIC_LOGS, 0);
            if (failCount > 0) {
                throw new BusinessException("delete storage clickhouse error ,failCount " + failCount);
            }
            JobUtil.infoLog("delete clickhouse success");
        } catch (Exception be) {
            JobUtil.errorLog(be.getMessage());
            return ReturnT.FAIL;
        }

        return ReturnT.SUCCESS;
    }

    /**
     * 清除所有report metric数据 druid库
     *
     * @param params{"maxdays":365}
     */
    @XxlJob("deleteAllReportAndMetricsDataJobHandler")
    public ReturnT<String> deleteAllReportAndMetricsData(String params) {
        try {
            StorageQuota storageQuota = storageQuotaService.getStorageQuota(Constant.METRICS);
            Integer maxDays = getMaxDays(params, storageQuota.getMax_days());
            String druidHost = UrlUtil.getUrl(deletionConfig.getAnalyticServer());
            String deleteEndDate = DateUtils.getSomeDate(TimeConstants.YYYY_MM_DD, 1);
            String deleteStartTime = getDruidDataStartTime();
            //1停止加载
            String changeRulesUrl = String.join("",druidHost, "/druid/coordinator/v1/rules/");
            String changeRulesParam = "[{ \"type\":\"dropByInterval\", \"interval\":\"" + deleteStartTime + "/" + deleteEndDate + "\" }]";
            //2标记unused
            String unUseParam = String.join("","{\"interval\":\"", deleteStartTime, "/", deleteEndDate, "\"}");
            JobUtil.infoLog("reset storage days druid unused command = {}", unUseParam);
            //3物理删除所有数据
            String deleteUrl = String.join("",druidHost, "/druid/indexer/v1/task");
            String deleteParamTemp = String.join("","{\"type\":\"kill\",\"dataSource\":\"{}\",\"interval\":\"", deleteStartTime + "/", deleteEndDate, "\"}");
            //4恢复加载规则
            String resetRulesParam = StrUtil.format("[{\"type\":\"loadByPeriod\",\"period\":\"P{}D\",\"tieredReplicants\":{\"_default_tier\":1}},{\"type\":\"dropForever\"}]", maxDays);

            int failCount = 0;
            for (String table : storageQuota.getSource_list()) {
                try {
                    //每个表限制操作时间五分钟，开始计时
                    long startTime = System.currentTimeMillis();
                    long endTime = startTime + 1000 * 60 * 5;
                    JobUtil.infoLog("-----------------reset storage druid table = {} ------------------", table);
                    XxlJobLogger.log("delete druid table first command , endpoint = {}, table = {}, sql = {}", druidHost, table, changeRulesParam);
                    HttpClientUtils.httpPost(changeRulesUrl + table, changeRulesParam);

                    //查询表数据是否删除 固定key EXPR$0
                    String countTableDataSql = Joiner.on("").join("{\"query\":\"SELECT count(*) from ", table, " where __time >= '", deleteStartTime, "' AND __time <= '",
                            getDruidDataEndTime(UrlUtil.getUrl(druidHost, analyticUrl)), "'\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}");
                    int count = 1;
                    while (count > 0) {
                        String httpPostResult = HttpClientUtils.httpPost(UrlUtil.getUrl(druidHost, analyticUrl), countTableDataSql);
                        List<Map> list = (List) JsonMapper.fromJsonString(httpPostResult, List.class);
                        count = Integer.valueOf(String.valueOf(list.get(0).get("EXPR$0")));
                        if (count > 0) {
                            //每次停留5秒判断是否删除结束
                            ThreadUtil.sleep(5000L);
                        }
                        if (System.currentTimeMillis() > endTime) {
                            logger.error("delete druid time out error, table = {} ", table);
                            XxlJobLogger.log("delete druid time out error, table = {} ", table);
                            break;
                        }
                    }

                    String unUseMessage = HttpClientUtils.httpPost(UrlUtil.getUrl(deletionConfig.getAnalyticServer(), "/druid/coordinator/v1/datasources/", table, "/markUnused"), unUseParam);
                    JobUtil.infoLog("reset storage days druid unused result message :{}", unUseMessage);

                    String deleteParams = StrUtil.format(deleteParamTemp, table);
                    JobUtil.infoLog("delete druid table second command table = {}, sql = {}", table, deleteParams);

                    String deleteMessage = HttpClientUtils.httpPost(deleteUrl, deleteParams);
                    JobUtil.infoLog("delete druid table third command table = {}, sql = {}", table, resetRulesParam);
                    HttpClientUtils.httpPost(changeRulesUrl + table, resetRulesParam);

                    XxlJobLogger.log("delete druid table result message = {}", deleteMessage);
                } catch (BusinessException e) {
                    // 表不存在时，查询发生错误，记录下继续执行
                    JobUtil.errorLog(" query druid error, endpoint = {}, table = {}, fail message = {}", deletionConfig.getAnalyticServer(), table, JobUtil.getErrorMsg(e));
                    failCount++;
                }
            }
            modifyLastStorage(Constant.ZK_REPORT_AND_METRICS, 0);
            if (failCount > 0) {
                throw new BusinessException("delete druid error, fail count " + failCount);
            }
        } catch (Exception be) {
            logger.error(be.getMessage());
            XxlJobLogger.log(be.getMessage());
            return ReturnT.FAIL;
        }

        return ReturnT.SUCCESS;
    }

    /**
     * 清除文件数据hbase库
     *
     * @param params{"maxdays":365}
     */
    @XxlJob("deleteAllFilesJobHandler")
    public ReturnT<String> deleteAllFiles(String params) {
        try {
            StorageQuota storageQuota = storageQuotaService.getStorageQuota(Constant.FILES);
            Header header = new BasicHeader(Constant.TOKEN, deletionConfig.getFilesToken());
            List<String> endpointList = Splitter.on(",").trimResults()
                    .omitEmptyStrings()
                    .splitToList(deletionConfig.getFilesHosServer());

            int failCount = 0;
            for (String endpoint : endpointList) {
                try {
                    String filesServer = UrlUtil.getUrl(endpoint);
                    JobUtil.infoLog("delete file endpoint = {}, bucket list = {} ", endpoint, storageQuota.getSource_list());
                    for (String bucketName : storageQuota.getSource_list()) {
                        try {
                            HttpClientUtils.httpDelete(String.join("",filesServer, "/hos/", bucketName, "?truncate"), header);
                        } catch (BusinessException e) {
                            failCount++;
                            JobUtil.errorLog("endpoint = {},bucket ={}, fail message={}", endpoint, bucketName, JobUtil.getErrorMsg(e));
                        }
                    }
                } catch (BusinessException e) {
                    failCount++;
                    JobUtil.errorLog("endpoint = {}, fail message={}", endpoint, JobUtil.getErrorMsg(e));
                }
            }
            modifyLastStorage(Constant.FILES, 0);
            if (failCount > 0) {
                throw new BusinessException(" delete files error, failed count " + failCount);
            }
        } catch (Exception be) {
            JobUtil.errorLog(be.getMessage());
            return ReturnT.FAIL;
        }
        return ReturnT.SUCCESS;
    }

    //公共查询clickhouse
    private List<String> getSystemDataForCK(String url, String sql) {
        List<String> dataList = Lists.newArrayList();
        Map<String, Object> deleteParamMap = deletionConfig.getCkSource();
        deleteParamMap.put("query", sql);
        String httpGetResult = HttpClientUtils.httpGet(HttpClientUtils.getUrlWithParams(url, deleteParamMap));

        if ("-1".equals(httpGetResult)) {
            throw new BusinessException("http request error url = " + url);
        }
        Map httpGetResultMap = (Map) JsonMapper.fromJsonString(httpGetResult, Map.class);
        List<Map> list = (List) httpGetResultMap.get("data");
        list.stream().filter(Objects::nonNull).forEach(map -> dataList.addAll(map.values()));
        return dataList;
    }

    /**
     * 查询Druid数据的开始时间,为空时返回今天，格式yyyy-MM-dd
     */
    private String getDruidDataStartTime() {
        //查询druid数据库的数据的开始时间
        String queryAnalyticStartTimeSql = "{\"query\":\"SELECT \\\"start\\\" FROM sys.segments WHERE \\\"start\\\" <= '" + DateUtils.getSomeDate(DateUtils.YYYY_MM_DD, 1) + "' order by \\\"start\\\" limit 1\"}";//durid 清库的起始时间
        String httpPost = HttpClientUtils.httpPost(UrlUtil.getUrl(deletionConfig.getAnalyticServer(), analyticUrl), queryAnalyticStartTimeSql);
        //查询表数据url
        List<Map> startTimeList = (List) JsonMapper.fromJsonString(httpPost, List.class);
        return ObjectUtils.isEmpty(startTimeList) ? DateUtils.getCurrentDate() :
                DateUtils.getFormatDate(DateUtils.convertStringToDate(String.valueOf(startTimeList.get(0).get("start")), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"), DateUtils.YYYY_MM_DD);
    }

    /**
     * 更新标准时间
     *
     * @param logType
     * @param maxDays
     */
    private void modifyLastStorage(String logType, Integer maxDays) {
        // 设置标准时间,如果now - 标准时间 > maxDays,则更新 标准时间 = 当前时间-maxday
        Long lastStorage = getLastStorage(logType);
        Long now = System.currentTimeMillis() / 1000;
        Long max = maxDays * 24 * 60 * 60L;// 单位秒
        if (now - lastStorage > max) {
            ZookeeperUtils zk = new ZookeeperUtils();
            zk.modifyNode(Constant.ZOOKEEPER_STORAGE_PATH + logType, String.valueOf(now - max), zookeeperServer);
        }
    }

    private String getDruidDataEndTime(String url) {
        //查询druid数据库的数据的结束时间
        String queryAnalyticStartTimeSql = "{\"query\":\"SELECT version FROM sys.segments WHERE version LIKE '2%' ORDER BY version DESC LIMIT 1\"}";//durid 清库的起始时间
        String httpPost = HttpClientUtils.httpPost(url, queryAnalyticStartTimeSql);
        //查询表数据url
        List<Map> startTimeList = (List) JsonMapper.fromJsonString(httpPost, List.class);
        return ObjectUtils.isEmpty(startTimeList) ? DateUtils.getCurrentDate() : String.valueOf(startTimeList.get(0).get("version"));
    }

    /***
     * --查所有的IP地址
     * @Date 2020\9\18 0018
     * @return java.util.List<java.lang.String>
     **/
    private List<String> getAddressForCK(String url) {
        List<String> endpointList = new ArrayList<>();
        endpointList.addAll(getSystemDataForCK(url, Joiner.on("").join("SELECT DISTINCT concat(host_address,':','" + trafficPort + "') as endpoint FROM ", deletionConfig.getSystemClusters(), " FORMAT JSON;")));
        return endpointList;
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
            String nodeData = zk.getNodeData(Constant.ZOOKEEPER_STORAGE_PATH + node, zookeeperServer);
            //不存在创建一个
            if (org.springframework.util.ObjectUtils.isEmpty(nodeData)) {
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
