package com.mesalab.executor.jobhandler;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mesalab.executor.core.config.StorgeConfig;
import com.mesalab.executor.core.utils.*;
import com.mesalab.executor.exception.BusinessException;
import com.mesalab.executor.pojo.LifecycleConfiguration;
import com.mesalab.executor.pojo.ListAllMyBucketsResult;
import com.mesalab.executor.service.StorageQuotaService;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import com.zdjizhi.utils.*;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class LogStorageQuotaJob {

    private static Logger logger = LoggerFactory.getLogger(LogStorageQuotaJob.class);

    private static String trafficDataSource;
    private static String trafficUserName;
    private static String trafficPassword;

    private static String trafficServer;
    private static final String trafficPort = "8123";
    private static String analyticServerSelect;
    private static String analyticServerDelete;

    private static String systemParts;
    private static String systemClusters;
    private static String systemTables;

    private static String filesHosServer;
    private static String filesToken;


    @Autowired
    StorgeConfig storgeConfig;

    @Autowired
    StorageQuotaService storageQuotaService;


    @Autowired
    public LogStorageQuotaJob(StorgeConfig deletionConfig) {
        //clickhouse
        trafficServer = deletionConfig.getTrafficServer();
        trafficDataSource = deletionConfig.getTrafficDatasource();
        trafficUserName = deletionConfig.getTrafficUsername();
        trafficPassword = deletionConfig.getTrafficPassword();
        systemParts = deletionConfig.getSystemParts();
        systemClusters = deletionConfig.getSystemClusters();
        systemTables = deletionConfig.getSystemTables();

        //druid
        analyticServerSelect = Joiner.on("").join(Constant.HTTP, deletionConfig.getAnalyticServer(), "/druid/v2/sql");
        analyticServerDelete = deletionConfig.getAnalyticServer();

        //hos
        filesHosServer = deletionConfig.getFilesHosServer();
        filesToken = deletionConfig.getFilesToken();
    }

    //*********************************************设置存储策略**********************************************************


    /**
     * 设置流量日志存储策略储
     * clickhouse 数据库
     *
     * @param params {"maxdays":30}
     */
    @XxlJob("deleteTrafficDataJobHandler")
    public ReturnT<String> deleteTrafficData(String params) {
        try {
            Map<String, Object> paramsMap = validParams(params);
            if (ObjectUtils.isEmpty(paramsMap)) {
                logger.error("params parser error , params is {}", params);
                return IJobHandler.FAIL;
            }
            Map<String, Object> deleteParamMap = getDeleteSource();

            //根据服务器地址，表，分区去删除数据
            String prefixDeleteSql = Joiner.on("").join("ALTER TABLE ", trafficDataSource, ".");
            String deleteMaxDate = DateUtils.getSomeDate(TimeConstants.YYYYMMDD, -Integer.valueOf(String.valueOf(paramsMap.get("maxdays"))));
            List<String> addressForCKs = getAddressForCK(Constant.HTTP.concat(trafficServer));
            int failCount = 0;
            // 指定服务器
            for (String endpoint : addressForCKs) {
                try {
                    // 指定表
                    String url = Constant.HTTP.concat(endpoint);
                    List<String> tablesForCKs = getTablesForCK(url);
                    for (String table : tablesForCKs) {
                        try {
                            //指定分区
                            List<String> partitions = getSystemDataForCK(url, Joiner.on("").join("SELECT DISTINCT(`partition`) from " + systemParts + " WHERE table = '", table, "' AND `partition` < '", deleteMaxDate, "' FORMAT JSON;"));
                            for (String partition : partitions) {
                                try {
                                    deleteParamMap.put("query", Joiner.on("").join(prefixDeleteSql, table, " DROP PARTITION ", partition));
                                    HttpClientUtils.httpPost(HttpClientUtils.getUrlWithParams(url, deleteParamMap), null);
                                } catch (Exception e) {
                                    failCount++;
                                    logger.error("clickhouse ttl error endpoint = {}, table = {}, partition = {}, fail message = {}", endpoint, table, partition, JobUtil.getErrorMsg(e));
                                    XxlJobLogger.log("clickhouse ttl error partitionFailCount={}, endpoint = {}, table = {}, partition = {}, fail message = {}", failCount, endpoint, table, partition, JobUtil.getErrorMsg(e));
                                }
                            }
                            logger.info("clickhouse ttl clickhouse endpoint = {}, table = {}, partition < {}, size = {}, fail count = {}", endpoint, table, deleteMaxDate, partitions.size(), failCount);
                        } catch (Exception e) {
                            failCount++;
                            logger.error("clickhouse ttl error endpoint = {},table = {}, fail message = {}", endpoint, table, JobUtil.getErrorMsg(e));
                            XxlJobLogger.log("clickhouse ttl error endpoint = {},table = {}, fail message = {}", endpoint, table, JobUtil.getErrorMsg(e));
                        }
                    }
                    XxlJobLogger.log("clickhouse ttl endpoint = {}, failCount = {}, tables = {}", endpoint, failCount, tablesForCKs);
                } catch (Exception e) {
                    failCount++;
                    logger.error("clickhouse ttl error endpoint = {}, fail message = {}", endpoint, JobUtil.getErrorMsg(e));
                    XxlJobLogger.log("clickhouse ttl error endpoint = {}, fail message = {}", endpoint, JobUtil.getErrorMsg(e));
                }
            }
            modifyLastStorage(Constant.ZK_TRAFFIC_LOGS, Integer.valueOf(String.valueOf(paramsMap.get("maxdays"))));
            if (failCount > 0) {
                throw new BusinessException("reset storage clickhouse ttl error ,failCount " + failCount);
            }

        } catch (BusinessException be) {
            logger.error(be.getErrorMessage());
            XxlJobLogger.log(be.getErrorMessage());
            return ReturnT.FAIL;
        } catch (Exception e) {
            logger.error(e.getMessage());
            XxlJobLogger.log(e);
            return ReturnT.FAIL;
        }
        return ReturnT.SUCCESS;
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
            Map<String, Object> paramsMap = validParams(params);
            if (ObjectUtils.isEmpty(paramsMap)) {
                logger.error("params parser error , params is {}", params);
                return IJobHandler.FAIL;
            }

            String queryTablesSql = "{\"query\":\"SELECT DISTINCT(datasource) FROM sys.tasks WHERE datasource NOT LIKE '%hot%'\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";
            modifyLastStorage(Constant.ZK_REPORT_AND_METRICS, Integer.valueOf(String.valueOf(paramsMap.get("maxdays"))));
            deleteDruidTable(queryTablesSql, Integer.valueOf(String.valueOf(paramsMap.get("maxdays"))), false);

        } catch (BusinessException be) {
            logger.error(be.getErrorMessage());
            XxlJobLogger.log(be.getErrorMessage());
            return ReturnT.FAIL;
        } catch (Exception e) {
            logger.error(e.getMessage());
            XxlJobLogger.log(e);
            return ReturnT.FAIL;
        }

        return ReturnT.SUCCESS;
    }

    /**
     * 日志存储策略
     * druid 数据库
     * 用于删除hot表，默认保留一天
     *
     * @param params{"maxdays":1}
     */
    @XxlJob("deleteDruidHotLogJobHandler")
    public ReturnT<String> deleteDruidHotLogJobHandler(String params) {

        try {
            Map<String, Object> paramsMap = validParams(params);
            if (ObjectUtils.isEmpty(paramsMap)) {
                logger.error("params parser error , params is {}", params);
                return IJobHandler.FAIL;
            }
            String queryTablesSql = "{\"query\":\"SELECT DISTINCT(datasource) FROM sys.tasks  WHERE datasource LIKE '%hot%'\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";
            deleteDruidTable(queryTablesSql, Integer.valueOf(String.valueOf(paramsMap.get("maxdays"))), true);
        } catch (BusinessException be) {
            logger.error(be.getErrorMessage());
            XxlJobLogger.log(be.getErrorMessage());
            return ReturnT.FAIL;
        } catch (Exception e) {
            logger.error(e.getMessage());
            XxlJobLogger.log(e);
            return ReturnT.FAIL;
        }

        return ReturnT.SUCCESS;
    }

    /*
     * @Author zhq
     * @Description //TODO 通过查询跟天数，查到的表保留天数
     * @Date 2020\9\25
     * @Param [queryTablesSql, maxdays]
     * @return void
     **/
    private void deleteDruidTable(String queryTablesSql, Integer maxdays, boolean hotTask) {
        List<String> analyticTables = getAnalyticTables(analyticServerSelect, queryTablesSql);
        //修改规则
        String changeRulesUrl = Joiner.on("").join("http://", analyticServerDelete, "/druid/coordinator/v1/rules/");
        String changeRulesParam = Joiner.on("").join("[{\"type\":\"loadByPeriod\",\"period\":\"P", maxdays, "D\",\"tieredReplicants\":{\"_default_tier\":1}},{\"type\":\"dropForever\"}]");

        //1.物理删除除
        String deleteUrl = Joiner.on("").join(Constant.HTTP, analyticServerDelete, "/druid/indexer/v1/task");
        String prefixDeleteParam = "{\"type\":\"kill\",\"dataSource\":\"";
        //起始时间/当前时间-maxDay
        String druidDataStartTime = getDruidDataStartTime();
        String deleteMaxDate = DateUtils.getSomeDate(TimeConstants.YYYY_MM_DD, -maxdays);
        String suffixDeleteParam = Joiner.on("").join("\",\"interval\":\"", druidDataStartTime, "/", deleteMaxDate, "\"}");

        int failCount = 0;
        for (String table : analyticTables) {
            try {
                // hot表不用执行修改规则
                if (!hotTask) {
                    XxlJobLogger.log("reset storage days first command table :{}, sql : {}", table, changeRulesParam);
                    HttpClientUtils.httpPost(changeRulesUrl + table, changeRulesParam);
                }
                if (druidDataStartTime.compareTo(deleteMaxDate) < 0) {
                    XxlJobLogger.log("reset storage days druid command endpoint = {}, table = {}, sql = {}", analyticServerDelete, table, prefixDeleteParam + table + suffixDeleteParam);
                    logger.info("reset storage days druid second command endpoint = {}, table = {}, sql = {}", analyticServerDelete, table, prefixDeleteParam + table + suffixDeleteParam);
                    String deleteMessage = HttpClientUtils.httpPost(deleteUrl, prefixDeleteParam + table + suffixDeleteParam);
                    XxlJobLogger.log("reset storage days druid result endpoint = {}, table = {}, message :{}", analyticServerDelete, table, deleteMessage);
                }
            } catch (Exception e) {
                failCount++;
                logger.error("reset storage druid ttl endpoint = {}, table = {}, fail message = {}", analyticServerDelete, table, JobUtil.getErrorMsg(e));
                XxlJobLogger.log("reset storage druid ttl endpoint = {}, table = {}, fail message = {}", analyticServerDelete, table, JobUtil.getErrorMsg(e));
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
            Map<String, Object> paramsMap = validParams(params);
            if (ObjectUtils.isEmpty(paramsMap)) {
                logger.error("params parser error , params is {}", params);
                return IJobHandler.FAIL;
            }
            List<String> endpointList = Splitter.on(",").trimResults().omitEmptyStrings().splitToList(filesHosServer);

            Header header1 = new BasicHeader("Token", filesToken);
            Header header2 = new BasicHeader(HttpHeaders.CONTENT_TYPE, "text/xml");
            Header[] headers = {header1, header2};
            int failCount = 0;
            for (String endpoint : endpointList) {
                try {
                    String filesServer = Constant.HTTP.concat(endpoint);
                    String httpGetRes = HttpClientUtils.httpGet(Joiner.on("").join(filesServer, "/hos/"), header1);
                    ListAllMyBucketsResult list = XmlUtil.converXmlToBean(ListAllMyBucketsResult.class, httpGetRes);
                    logger.info("reset storage files ttl endpoint = {}, bucketList = {} ", endpoint, list);
                    XxlJobLogger.log("reset storage files ttl endpoint = {}, bucketList = {} ", endpoint, list);
                    if (ObjectUtils.allNotNull(list, list.getBuckets(), list.getBuckets().getBucket())) {
                        ListAllMyBucketsResult.Buckets buckets = list.getBuckets();
                        List<ListAllMyBucketsResult.Buckets.Bucket> bucketList = buckets.getBucket();

                        LifecycleConfiguration lc = new LifecycleConfiguration();
                        lc.getRule().setExpiration(new LifecycleConfiguration.Expiration(String.valueOf(paramsMap.get("maxdays"))));

                        for (ListAllMyBucketsResult.Buckets.Bucket bucket : bucketList) {
                            try {
                                HttpClientUtils.httpPut(Joiner.on("").join(filesServer, "/hos/", bucket.getName(), "?lifecycle"), XmlUtil.converToXml(lc), headers);
                            } catch (Exception e) {
                                failCount++;
                                logger.error("reset storage files ttl failCount = {}, endpoint = {}, bucket = {}, fail message = {}", failCount, endpoint, bucket, JobUtil.getErrorMsg(e));
                                XxlJobLogger.log("reset storage files ttl failCount = {}, endpoint = {}, bucket = {}, fail message = {}", failCount, endpoint, bucket, JobUtil.getErrorMsg(e));
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.error("reset storage files ttl endpoint = {}, fail message = {}", endpoint, JobUtil.getErrorMsg(e));
                    XxlJobLogger.log("reset storage files ttl endpoint = {}, fail message = {}", endpoint, JobUtil.getErrorMsg(e));
                    failCount++;
                }
            }
            modifyLastStorage(Constant.FILES, Integer.valueOf(String.valueOf(paramsMap.get("maxdays"))));
            if (failCount > 0) {
                throw new BusinessException(" number of total endpoints " + endpointList.size() + ", number of failed count " + failCount);
            }

        } catch (BusinessException be) {
            logger.error(be.getErrorMessage());
            XxlJobLogger.log(be.getErrorMessage());
            return ReturnT.FAIL;
        } catch (Exception e) {
            logger.error(e.getMessage());
            XxlJobLogger.log(e);
            return ReturnT.FAIL;
        }
        return ReturnT.SUCCESS;
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
            Map<String, Object> paramsMap = validParams(params);
            if (ObjectUtils.isEmpty(paramsMap)) {
                logger.error("params parser error , params is {}", params);
                return IJobHandler.FAIL;
            }
            List<String> addressForCKs = getAddressForCK(Constant.HTTP.concat(trafficServer));
            Map<String, Object> deleteParamMap = getDeleteSource();
            //清库命令参数
            String deleteSql = Joiner.on("").join("TRUNCATE TABLE IF EXISTS ", trafficDataSource, ".");
            int failCount = 0;
            for (String endpoint : addressForCKs) {
                try {
                    String url = Constant.HTTP.concat(endpoint);
                    List<String> tablesForCKs = getTablesForCK(url);
                    for (String table : tablesForCKs) {
                        try {
                            deleteParamMap.put("query", deleteSql.concat(table));
                            logger.info("delete clickhouse endpoint = {}, sql:{}", endpoint, deleteSql.concat(table));
                            HttpClientUtils.httpPost(HttpClientUtils.getUrlWithParams(url, deleteParamMap), null);
                        } catch (Exception e) {
                            logger.error("delete storage clickhouse error endpoint = {}, table = {}, fail message = {}", endpoint, table, JobUtil.getErrorMsg(e));
                            XxlJobLogger.log("delete storage clickhouse error endpoint = {}, table = {}, fail message = {}", endpoint, table, JobUtil.getErrorMsg(e));
                            failCount++;
                        }
                    }
                    XxlJobLogger.log("delete storage clickhouse error endpoint = {}, fail count = {}, tables = {}", endpoint, failCount, tablesForCKs);
                } catch (Exception e) {
                    logger.error("delete storage clickhouse error endpoint = {},  fail message = {}", endpoint, JobUtil.getErrorMsg(e));
                    XxlJobLogger.log("delete storage clickhouse error endpoint = {},  fail message = {}", endpoint, JobUtil.getErrorMsg(e));
                    failCount++;
                }
            }
            modifyLastStorage(Constant.ZK_TRAFFIC_LOGS, 0);
            if (failCount > 0) {
                throw new BusinessException("delete storage clickhouse error ,failCount " + failCount);
            }

        } catch (BusinessException be) {
            logger.error(be.getErrorMessage());
            XxlJobLogger.log(be.getErrorMessage());
            return ReturnT.FAIL;
        } catch (Exception e) {
            logger.error(e.getMessage());
            XxlJobLogger.log(e);
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
            Map<String, Object> paramsMap = validParams(params);
            if (ObjectUtils.isEmpty(paramsMap)) {
                logger.error("params parser error , params is {}", params);
                return IJobHandler.FAIL;
            }
            String deleteEndDate = DateUtils.getSomeDate(TimeConstants.YYYY_MM_DD, 1);

            String deleteendpoint = Constant.HTTP.concat(analyticServerDelete);

            String deleteStartTime = getDruidDataStartTime();
            //1停止加载
            String changeRulesUrl = Joiner.on("").join(deleteendpoint, "/druid/coordinator/v1/rules/");
            String changeRulesParam = "[{ \"type\":\"dropByInterval\", \"interval\":\"" + deleteStartTime + "/" + deleteEndDate + "\" }]";

            //2物理删除所有数据
            String deleteUrl = Joiner.on("").join(deleteendpoint, "/druid/indexer/v1/task");
            String prefixDeleteParam = "{\"type\":\"kill\",\"dataSource\":\"";
            String suffixDeleteParam = Joiner.on("").join("\",\"interval\":\"", deleteStartTime + "/", deleteEndDate, "\"}");
            //3恢复加载规则
            String resetRulesParam = Joiner.on("").join("[{\"type\":\"loadByPeriod\",\"period\":\"P", paramsMap.get("maxdays"), "D\",\"tieredReplicants\":{\"_default_tier\":1}},{\"type\":\"dropForever\"}]");
            //清库时注意不操作hot结尾的表
            String queryTablesSql = "{\"query\":\"SELECT DISTINCT(datasource) FROM sys.tasks WHERE datasource NOT LIKE '%hot%'\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";
            List<String> analyticTables = getAnalyticTables(analyticServerSelect, queryTablesSql);
            int failCount = 0;
            for (String table : analyticTables) {
                try {
                    //每个表限制操作时间五分钟，开始计时
                    long startTime = System.currentTimeMillis();
                    long endTime = startTime + 1000 * 60 * 5;
                    XxlJobLogger.log("delete druid table first command , endpoint = {}, table = {}, sql = {}", analyticServerDelete, table, changeRulesParam);
                    HttpClientUtils.httpPost(changeRulesUrl + table, changeRulesParam);

                    //查询表数据是否删除 固定key EXPR$0
                    String countTableDataSql = Joiner.on("").join("{\"query\":\"SELECT count(*) from ", table, " where __time >= '", deleteStartTime, "' AND __time <= '", getDruidDataEndTime(analyticServerSelect), "'\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}");
                    int count = 1;
                    while (count > 0) {
                        String httpPostResult = HttpClientUtils.httpPost(analyticServerSelect, countTableDataSql);
                        List<Map> list = (List) JsonMapper.fromJsonString(httpPostResult, List.class);
                        count = Integer.valueOf(String.valueOf(list.get(0).get("EXPR$0")));
                        try {
                            if (count > 0) {
                                //每次停留5秒判断是否删除结束
                                Thread.sleep(5000L);
                            }
                            if (System.currentTimeMillis() > endTime) {
                                logger.error("delete druid time out error, table = {} ", table);
                                XxlJobLogger.log("delete druid time out error, table = {} ", table);
                                break;
                            }
                        } catch (InterruptedException e) {
                            logger.error(" query druid error {}", e.getMessage());
                            XxlJobLogger.log(" query druid error {}", e.getMessage());
                        }
                    }
                    XxlJobLogger.log("delete druid table second command table = {}, sql = {}", table, prefixDeleteParam + table + suffixDeleteParam);
                    String deleteMessage = HttpClientUtils.httpPost(deleteUrl, prefixDeleteParam + table + suffixDeleteParam);
                    XxlJobLogger.log("delete druid table third command table = {}, sql = {}", table, resetRulesParam);
                    HttpClientUtils.httpPost(changeRulesUrl + table, resetRulesParam);

                    XxlJobLogger.log("delete druid table result message = {}", deleteMessage);
                } catch (Exception e) {
                    // 表不存在时，查询发生错误，记录下继续执行
                    logger.error(" query druid error, table = {}, fail message = {}", table, JobUtil.getErrorMsg(e));
                    XxlJobLogger.log(" query druid error, endpoint = {}, table = {}, fail message = {}", analyticServerDelete, table, JobUtil.getErrorMsg(e));
                    failCount++;
                }
            }
            modifyLastStorage(Constant.ZK_REPORT_AND_METRICS, 0);
            if (failCount > 0) {
                throw new BusinessException("delete druid error, fail count " + failCount);
            }
        } catch (BusinessException be) {
            logger.error(be.getErrorMessage());
            XxlJobLogger.log(be.getErrorMessage());
            return ReturnT.FAIL;
        } catch (Exception e) {
            logger.error(e.getMessage());
            XxlJobLogger.log(e);
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
            Map<String, Object> paramsMap = validParams(params);
            if (ObjectUtils.isEmpty(paramsMap)) {
                logger.error("params parser error , params is {}", params);
                return IJobHandler.FAIL;
            }
            Header header = new BasicHeader("Token", filesToken);
            List<String> endpointList = Splitter.on(",").trimResults().omitEmptyStrings().splitToList(filesHosServer);

            int failCount = 0;
            for (String endpoint : endpointList) {
                try {
                    String filesServer = Constant.HTTP.concat(endpoint);
                    String httpGetRes = HttpClientUtils.httpGet(Joiner.on("").join(filesServer, "/hos/"), header);
                    ListAllMyBucketsResult list = XmlUtil.converXmlToBean(ListAllMyBucketsResult.class, httpGetRes);
                    logger.info("delete storage files ttl endpoint = {}, bucket list = {} ", endpoint, list);
                    XxlJobLogger.log("query file endpoint = {}, bucket list = {} ", endpoint, list);

                    if (ObjectUtils.allNotNull(list, list.getBuckets(), list.getBuckets().getBucket())) {
                        ListAllMyBucketsResult.Buckets buckets = list.getBuckets();
                        List<ListAllMyBucketsResult.Buckets.Bucket> bucketList = buckets.getBucket();
                        XxlJobLogger.log("delete file endpoint = {}, bucket list = {} ", endpoint, bucketList);
                        logger.info("delete file endpoint = {}, bucket list = {} ", endpoint, bucketList);
                        for (ListAllMyBucketsResult.Buckets.Bucket bucket : bucketList) {
                            try {
                                HttpClientUtils.httpDelete(Joiner.on("").join(filesServer, "/hos/", bucket.getName(), "?truncate"), header);
                            } catch (Exception e) {
                                failCount++;
                                logger.error("delete file error endpoint = {},bucket ={}, fail message={}", endpoint, bucket, JobUtil.getErrorMsg(e));
                                XxlJobLogger.log("delete file error endpoint = {},bucket ={}, fail message={}", endpoint, bucket, JobUtil.getErrorMsg(e));
                            }
                        }
                    }
                } catch (Exception e) {
                    failCount++;
                    logger.error("delete file error endpoint = {}, fail message={}", endpoint, JobUtil.getErrorMsg(e));
                    XxlJobLogger.log("delete file error endpoint = {}, fail message={}", endpoint, JobUtil.getErrorMsg(e));

                }
            }
            modifyLastStorage(Constant.FILES, 0);
            if (failCount > 0) {
                throw new BusinessException(" delete files error, failed count " + failCount);
            }
        } catch (BusinessException be) {
            logger.error(be.getErrorMessage());
            XxlJobLogger.log(be.getErrorMessage());
            return ReturnT.FAIL;
        } catch (Exception e) {
            logger.error(e.getMessage());
            XxlJobLogger.log(e);
            return ReturnT.FAIL;
        }
        return ReturnT.SUCCESS;
    }


    /**
     * 直接查询数据库获取参数
     *
     * @param params {"topic":"SYS-STORAGE-LOG","ckDayGrowth":"false"}
     *               ckDayGrowth:表示是否为一天的执行一次的时间粒度
     */
    @XxlJob("getStorageQuotaJobHandler")
    public ReturnT<String> getStorageQuota(String params) {

        try {
            Map<String, Object> paramsMap = validParams(params);
            if (ObjectUtils.isEmpty(paramsMap)) {
                logger.error("params parser error , params is {}", params);
                return IJobHandler.FAIL;
            }
            //调用查询方法单机，集群
            Map resultMap = null;

            if ("false".equals(paramsMap.get("ckDayGrowth"))) {
                resultMap = storageQuotaService.getDiskJson();
            } else {
                resultMap = storageQuotaService.getDayJson();
            }

            //发送到kafka
            new KafkaUtils().sendMessage(String.valueOf(paramsMap.get("topic")), (List<Object>) resultMap.get("data"));
            XxlJobLogger.log("send message to kafka, topic is {}", paramsMap.get("topic"));
            //查询存在问题会返回status>0
            int status = ((Map<String, Integer>) resultMap.get("status")).values().stream().mapToInt(Integer::intValue).sum();
            if (status > 0) {
                return new ReturnT<String>(IJobHandler.FAIL.getCode(), " query error " + resultMap.get("status"));
            }

        } catch (BusinessException be) {
            logger.error(be.getErrorMessage());
            XxlJobLogger.log(be.getErrorMessage());
            return ReturnT.FAIL;
        } catch (Exception e) {
            logger.error(e.getMessage());
            XxlJobLogger.log(e);
            return ReturnT.FAIL;
        }

        return ReturnT.SUCCESS;
    }

    /**
     * 数据库连接参数
     *
     * @return map
     * {database：,
     * password：,
     * user:,
     * }
     */
    private Map<String, Object> getDeleteSource() {
        //删除sql参数拼接
        Map<String, Object> deleteParamMap = Maps.newHashMap();
        deleteParamMap.put("database", trafficDataSource);
        deleteParamMap.put("password", trafficPassword);
        deleteParamMap.put("user", trafficUserName);
        return deleteParamMap;
    }

    /**
     * 必填参数进行验证，其它自行配置
     *
     * @param params
     */
    private Map<String, Object> validParams(String params) {
        logger.info("params{}", params);

        if (StringUtil.isBlank(params)) {
            XxlJobLogger.log("params is Empty !");
            return null;
        }
        Map<String, Object> paramMap = (Map) JsonMapper.fromJsonString(params, Map.class);
        if (paramMap == null) {
            XxlJobLogger.log("params error !");
            return null;
        }
        return paramMap;
    }


    /**
     * 根据druid日志表
     */
    private List<String> getAnalyticTables(String url, String sql) {
        List<String> tableList = Lists.newArrayList();
        //查询表名
        String httpPostResult = HttpClientUtils.httpPost(url, sql);
        List<Map> list = (List) JsonMapper.fromJsonString(httpPostResult, List.class);
        list.forEach(map -> tableList.addAll(map.values()));
        return tableList;
    }

    /***
     * --查所有的IP地址
     * @Date 2020\9\18 0018
     * @return java.util.List<java.lang.String>
     **/
    private List<String> getAddressForCK(String url) {
        List<String> endpointList = new ArrayList<>();
        endpointList.addAll(getSystemDataForCK(url, Joiner.on("").join("SELECT DISTINCT concat(host_address,':','" + trafficPort + "') as endpoint FROM ", systemClusters, " FORMAT JSON;")));
        return endpointList;
    }

    /**
     * 查询clickhouse 需要删除的表
     *
     * @return List<String>
     */
    private List<String> getTablesForCK(String url) {
        return getSystemDataForCK(url, Joiner.on("").join("SELECT DISTINCT(name) FROM ", systemTables, " WHERE database = '", trafficDataSource, "' AND engine in  ('MergeTree','ReplicatedMergeTree') FORMAT JSON;"));
    }

    //公共查询clickhouse
    private List<String> getSystemDataForCK(String url, String sql) {
        List<String> dataList = Lists.newArrayList();
        Map<String, Object> deleteParamMap = getDeleteSource();
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
        String httpPost = HttpClientUtils.httpPost(analyticServerSelect, queryAnalyticStartTimeSql);
        //查询表数据url
        List<Map> startTimeList = (List) JsonMapper.fromJsonString(httpPost, List.class);
        return ObjectUtils.isEmpty(startTimeList) ? DateUtils.getCurrentDate() : DateUtils.getFormatDate(DateUtils.convertStringToDate(String.valueOf(startTimeList.get(0).get("start")), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"), DateUtils.YYYY_MM_DD);
    }

    /**
     * 更新标准时间
     *
     * @param logType
     * @param maxdays
     */
    private void modifyLastStorage(String logType, Integer maxdays) {
        // 设置标准时间,如果now - 标准时间 > maxdays,则更新 标准时间 = 当前时间-maxday
        Long lastStorage = storageQuotaService.getLastStorage(logType);
        Long now = System.currentTimeMillis() / 1000;
        Long max = maxdays * 24 * 60 * 60L;// 单位秒
        if (now - lastStorage > max) {
            storageQuotaService.modifyLastStorage(logType, String.valueOf(now - max));
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

}
