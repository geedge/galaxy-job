package com.mesalab.executor.jobhandler;

import cn.hutool.core.net.url.UrlBuilder;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.StrUtil;
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
import com.mesalab.executor.pojo.ListAllMyBucketsResult;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Component
public class LogStorageQuotaJob {

    private Log logger = Log.get();

    private static final String analyticUrl = "/druid/v2/sql";
    private static final String ck_cluster = "ck_cluster";
    private static final String max_days = "maxDays";
    private static final String trafficPort = "8123";

    @Value("${zookeeper.server}")
    private String zookeeperServer;

    private StorgeConfig deletionConfig = (StorgeConfig) SpringContextUtil.getBean("storgeConfig");
    //*********************************************设置存储策略**********************************************************
    private final Header[] hosHeaders = {new BasicHeader(Constant.TOKEN, deletionConfig.getFilesToken()),
            new BasicHeader(HttpHeaders.CONTENT_TYPE, Constant.TEXT_XML)};

    /**
     * 设置流量日志存储策略储
     * clickhouse
     */
    @XxlJob("deleteTrafficDataJobHandler")
    public ReturnT<String> deleteTrafficData(String params) {
        try {
            Map<String, Object> paramsMap = validParams(params);
            if (ObjectUtils.isEmpty(paramsMap)) {
                logger.error("params parser error , params is {}", params);
                return IJobHandler.FAIL;
            }
            Map<String, Object> deleteParamMap = deletionConfig.getCkSource();

            //根据服务器地址，表，分区去删除数据
            String prefixDeleteSql = Joiner.on("").join("ALTER TABLE ", deletionConfig.getTrafficDatasource(), ".");
            String deleteMaxDate = DateUtils.getSomeDate(TimeConstants.YYYYMMDD, -Integer.valueOf(String.valueOf(paramsMap.get(max_days))));
            String ckUrl = UrlBuilder.ofHttp(deletionConfig.getTrafficServer(), StandardCharsets.UTF_8).toString();
            List<String> addressForCKs = getAddressForCK(ckUrl);
            List<String> tablesForCKs = getTablesForCK(ckUrl,deletionConfig.getNotInSql("name",deletionConfig.getTafficExclusion()));
            int failCount = 0;
            // 指定服务器
            for (String endpoint : addressForCKs) {
                try {
                    JobUtil.infoLog("-----------------reset storage clickhouse ttl endpoint = {}  ------------------", endpoint);
                    String endpointUrl = UrlUtil.getUrl(endpoint);
                    for (String table : tablesForCKs) {
                        try {
                            //指定分区
                            List<String> partitions = getSystemDataForCK(endpointUrl, Joiner.on("").join("SELECT DISTINCT(`partition`) from " + deletionConfig.getSystemParts() + " WHERE table = '", table, "' AND `partition` < '", deleteMaxDate, "' FORMAT JSON;"));
                            for (String partition : partitions) {
                                try {
                                    deleteParamMap.put("query", Joiner.on("").join(prefixDeleteSql, table, " DROP PARTITION ", partition));
                                    HttpClientUtils.httpPost(HttpClientUtils.getUrlWithParams(endpointUrl, deleteParamMap), "");
                                } catch (BusinessException e) {
                                    failCount++;
                                    JobUtil.errorLog("clickhouse ttl error endpoint = {}, table = {}, partition = {}, fail message = {}", endpoint, table, partition, JobUtil.getErrorMsg(e));
                                }
                            }
                            JobUtil.infoLog("reset clickhouse ttl table = {}, partition < {}, size = {}, fail count = {}", table, deleteMaxDate, partitions, failCount);
                        } catch (BusinessException e) {
                            failCount++;
                            JobUtil.errorLog("clickhouse ttl error endpoint = {},table = {}, fail message = {}", endpoint, table, JobUtil.getErrorMsg(e));
                        }
                    }
                } catch (BusinessException e) {
                    failCount++;
                    JobUtil.errorLog("clickhouse ttl error endpoint = {}, fail message = {}", endpoint, JobUtil.getErrorMsg(e));
                }
            }
            modifyLastStorage(Constant.ZK_TRAFFIC_LOGS, Integer.valueOf(String.valueOf(paramsMap.get(max_days))));
            if (failCount > 0) {
                throw new BusinessException("reset storage clickhouse ttl error ,failCount " + failCount);
            }

        } catch (BusinessException be) {
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
            Map<String, Object> paramsMap = validParams(params);
            if (ObjectUtils.isEmpty(paramsMap)) {
                logger.error("params parser error , params is {}", params);
                return IJobHandler.FAIL;
            }
            if (ObjectUtils.isEmpty(paramsMap)) {
                logger.error("params parser error , params is {}", params);
                return IJobHandler.FAIL;
            }
            Map<String, Object> deleteParamMap = deletionConfig.getCkSource();

            String prefixDeleteSql = Joiner.on("").join("ALTER TABLE ", deletionConfig.getTrafficDatasource(), ".");
            String suffixDeleteSql = Joiner.on("").join(" ON CLUSTER ", ck_cluster, " DROP PARTITION ");
            Integer maxdays = Integer.valueOf(String.valueOf(paramsMap.get(max_days)));
            String deleteMaxDate = DateUtils.getSomeDate(TimeConstants.YYYYMMDD, -maxdays);

            String ckUrl = UrlBuilder.ofHttp(deletionConfig.getTrafficServer(), StandardCharsets.UTF_8).toString();
            List<String> tablesForCKs = getTablesForCK(ckUrl, deletionConfig.getNotInSql("name", deletionConfig.getTafficExclusion()));
            JobUtil.infoLog("reset storage clickhouse endpoint = {}, tables = {}", ckUrl, tablesForCKs);

            int failCount = 0;
            // 设置一个获取sql的方法，把表List
            for (String table : tablesForCKs) {

                try {
                    JobUtil.infoLog("-----------------reset storage clickhouse table = {}  ------------------", table);
                    List<String> partitionList = getSystemDataForCK(ckUrl, Joiner.on("").join("SELECT DISTINCT(`partition`) from " + deletionConfig.getSystemPartsCluster() + " WHERE table = '", table, "' AND `partition` < '", deleteMaxDate, "' FORMAT JSON;"));
                    JobUtil.infoLog("reset storage days table {}, drop partition size {} ", table, partitionList.size());

                    partitionList.forEach(partition -> {
                        String deleteSql = Joiner.on("").join(prefixDeleteSql, table, suffixDeleteSql, "'", partition, "'");
                        deleteParamMap.put("query", deleteSql);
                        JobUtil.infoLog("reset storage days clickhouse sql:{}", deleteSql);

                        HttpClientUtils.httpPost(HttpClientUtils.getUrlWithParams(ckUrl, deleteParamMap), null);
                        JobUtil.infoLog("reset storage days table {} partition {} success", table, partition);
                    });
                } catch (BusinessException e) {
                    failCount++;
                    JobUtil.errorLog("clickhouse ttl error table = {}, fail message = {}", table, JobUtil.getErrorMsg(e));
                }
            }
            modifyLastStorage(Constant.ZK_TRAFFIC_LOGS, maxdays);
            if (failCount > 0) {
                throw new BusinessException("reset storage clickhouse ttl error, failCount " + failCount);
            }
            JobUtil.infoLog("reset clickhouse {} days success", maxdays);
        } catch (BusinessException be) {
            JobUtil.errorLog(be.getMessage());
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

            String queryTablesSql = "{\"query\":\"SELECT DISTINCT(datasource) FROM sys.tasks WHERE datasource NOT LIKE '%hot%' " + deletionConfig.getNotInSql("datasource", deletionConfig.getAnalyticExclusion()) + " \",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";
            deleteDruidTable(queryTablesSql, Integer.valueOf(String.valueOf(paramsMap.get(max_days))), false);
            modifyLastStorage(Constant.ZK_REPORT_AND_METRICS, Integer.valueOf(String.valueOf(paramsMap.get(max_days))));

        } catch (BusinessException be) {
            logger.error(be.getMessage());
            XxlJobLogger.log(be.getMessage());
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
            String queryTablesSql = "{\"query\":\"SELECT DISTINCT(datasource) FROM sys.tasks  WHERE datasource LIKE '%hot%' " + deletionConfig.getNotInSql("datasource", deletionConfig.getAnalyticExclusion()) + " \",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";
            deleteDruidTable(queryTablesSql, Integer.valueOf(String.valueOf(paramsMap.get(max_days))), true);
        } catch (BusinessException be) {
            logger.error(be.getMessage());
            XxlJobLogger.log(be.getMessage());
            return ReturnT.FAIL;
        }

        return ReturnT.SUCCESS;
    }

    /*
     * @Author zhq
     * @Description 通过查询跟天数，查到的表保留天数
     * @Date 2020\9\25
     * @Param [queryTablesSql, maxdays]
     * @return void
     **/
    private void deleteDruidTable(String queryTablesSql, Integer maxdays, boolean hotTask) {
        List<String> analyticTables = getAnalyticTables(UrlUtil.getUrl(deletionConfig.getAnalyticServer(), analyticUrl), queryTablesSql);
        //1.修改规则
        String changeRulesUrl = UrlUtil.getUrl(deletionConfig.getAnalyticServer(), "/druid/coordinator/v1/rules/");
        String changeRulesParam = Joiner.on("").join("[{\"type\":\"loadByPeriod\",\"period\":\"P", maxdays, "D\",\"tieredReplicants\":{\"_default_tier\":1}},{\"type\":\"dropForever\"}]");
        // 2. unuse
        // curl -X 'POST' -H 'Content-Type:application/json' -d '{ "interval" : "2022-01-01/3000-01-03" }' /druid/coordinator/v1/datasources/$table_name/markUnused

        //3 .物理删除
        String deleteUrl = UrlUtil.getUrl(deletionConfig.getAnalyticServer(), "/druid/indexer/v1/task");
        String prefixDeleteParam = "{\"type\":\"kill\",\"dataSource\":\"";
        //起始时间/当前时间-maxDay
        String druidDataStartTime = getDruidDataStartTime();
        String deleteMaxDate = DateUtils.getSomeDate(TimeConstants.YYYY_MM_DD, -maxdays);
        String suffixDeleteParam = Joiner.on("").join("\",\"interval\":\"", druidDataStartTime, "/", deleteMaxDate, "\"}");
        String unUseParam = Joiner.on("").join("{\"interval\":\"", druidDataStartTime, "/", deleteMaxDate, "\"}");
        JobUtil.infoLog("reset storage days druid unused command = {}", unUseParam);

        int failCount = 0;
        for (String table : analyticTables) {
            try {
                JobUtil.infoLog("-----------------reset storage druid endpoint = {}, table = {} ------------------", deletionConfig.getAnalyticServer(), table);
                // hot表不用执行修改规则
                if (!hotTask) {
                    JobUtil.infoLog("reset storage days first rules command = {}", table, changeRulesParam);
                    HttpClientUtils.httpPost(changeRulesUrl + table, changeRulesParam);
                }
                if (druidDataStartTime.compareTo(deleteMaxDate) < 0) {

                    String unUseMessage = HttpClientUtils.httpPost(UrlUtil.getUrl(deletionConfig.getAnalyticServer(), "/druid/coordinator/v1/datasources/", table,"/markUnused") , unUseParam);
                    JobUtil.infoLog("reset storage days druid unused result message :{}", unUseMessage);

                    JobUtil.infoLog("reset storage days druid second command = {}",  prefixDeleteParam + table + suffixDeleteParam);
                    String deleteMessage = HttpClientUtils.httpPost(deleteUrl, prefixDeleteParam + table + suffixDeleteParam);
                    JobUtil.infoLog("reset storage days druid result message :{}", deleteMessage);
                }
            } catch (BusinessException e) {
                failCount++;
                JobUtil.errorLog("reset storage druid ttl table = {}, fail message = {}", table, JobUtil.getErrorMsg(e));
            }
        }
        if (ObjectUtils.isNotEmpty(deletionConfig.getAnalyticExclusion())) {
            List<String> tableExc = Splitter.on(",").trimResults().splitToList(deletionConfig.getAnalyticExclusion());
            for (String table : tableExc) {
                try {
                    JobUtil.infoLog("========================= load forever druid table {} =========================", table);
                    String loadMessage = HttpClientUtils.httpPost(changeRulesUrl + table, "[{\"type\":\"loadForever\",\"tieredReplicants\":{\"_default_tier\":1}}]");
                    JobUtil.infoLog("load forever druid result message {}", loadMessage);
                } catch (BusinessException e) {
                    failCount++;
                    JobUtil.errorLog("load forever storage druid ttl endpoint = {}, table = {}, fail message = {}", deletionConfig.getAnalyticServer(), table, JobUtil.getErrorMsg(e));
                }
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
            String maxDays = String.valueOf(paramsMap.get(max_days));
            List<String> hosHostList = parseList(deletionConfig.getFilesHosServer());
            int failCount = 0;
            for (String hosHost : hosHostList) {
                JobUtil.infoLog("-----------------reset storage hos endpoint = {} ------------------", hosHost);
                failCount += deleteHosData(hosHost, maxDays);
            }

            if (failCount > 0) {
                JobUtil.errorLog(" number of total endpoints " + hosHostList.size() + ", number of failed count " + failCount);
                return ReturnT.FAIL;
            }
        } catch (BusinessException be) {
            JobUtil.errorLog(be.getMessage());
            return ReturnT.FAIL;
        }
        return ReturnT.SUCCESS;
    }

    public int deleteHosData(String endpoint, String maxDays) {
        int failCount = 0;
        try {
            String hosServer = UrlUtil.getUrl(endpoint);
            // 获取桶列表
            List<String> bucketList = getBucketList(endpoint, hosServer);
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

    private List<String> getBucketList(String endpoint, String hosServer) {
        List<String> whiteList = parseList(deletionConfig.getFilesExclusion());
        String httpGetRes = HttpClientUtils.httpGet(hosServer + "/hos/", hosHeaders);
        if (httpGetRes == null || "-1".equals(httpGetRes)) {
            throw new BusinessException(StrUtil.format("reset storage files ttl endpoint = {} , fail message = I/O exception", endpoint));
        }

        List<String> bucketList = Lists.newArrayList();
        ListAllMyBucketsResult bucketsResult = XmlUtil.converXmlToBean(ListAllMyBucketsResult.class, httpGetRes);

        if (ObjectUtils.allNotNull(bucketsResult, bucketsResult.getBuckets(), bucketsResult.getBuckets().getBucket())) {
            for (ListAllMyBucketsResult.Buckets.Bucket bucket : bucketsResult.getBuckets().getBucket()) {
                if (ObjectUtils.isNotEmpty(whiteList) && whiteList.contains(bucket.getName())) {
                    continue;
                }
                bucketList.add(bucket.getName());
            }
        }
        return bucketList;
    }

    private List<String> parseList(String csv) {
        return StringUtil.isEmpty(csv) ? null : Splitter.on(",").trimResults().omitEmptyStrings().splitToList(csv);
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
            Map<String, Object> deleteParamMap = deletionConfig.getCkSource();
            //清库命令参数
            String deleteSql = Joiner.on("").join("TRUNCATE TABLE IF EXISTS ", deletionConfig.getTrafficDatasource(), ".");
            int failCount = 0;
            try {
                String url = UrlUtil.getUrl(deletionConfig.getTrafficServer());
                List<String> tablesForCKs = getTablesForCK(url, deletionConfig.getNotInSql("name", deletionConfig.getTafficExclusion()));
                logger.info("delete storage clickhouse endpoint = {}, tables = {}", url, tablesForCKs);
                XxlJobLogger.log("delete storage clickhouse endpoint = {}, tables = {}", url, tablesForCKs);

                for (String table : tablesForCKs) {
                    try {
                        String deleteSqlCluster = String.join(" ", deleteSql, table, " ON CLUSTER ", ck_cluster);
                        deleteParamMap.put("query", deleteSqlCluster);

                        logger.info("delete clickhouse table = {}, sql:{}", table, deleteSqlCluster);
                        HttpClientUtils.httpPost(HttpClientUtils.getUrlWithParams(url, deleteParamMap), "");
                        XxlJobLogger.log("delete clickhouse table = {}", table);

                    } catch (BusinessException e) {
                        logger.error("delete storage clickhouse error endpoint = {}, table = {}, fail message = {}", url, table, JobUtil.getErrorMsg(e));
                        XxlJobLogger.log("delete storage clickhouse error endpoint = {}, table = {}, fail message = {}", url, table, JobUtil.getErrorMsg(e));
                        failCount++;
                    }
                }
                logger.info("delete storage clickhouse endpoint = {}, fail count = {}, tables = {}", url, failCount, tablesForCKs);
                XxlJobLogger.log("delete storage clickhouse endpoint = {}, fail count = {}, tables = {}", url, failCount, tablesForCKs);

            } catch (BusinessException e) {
                logger.error("delete storage clickhouse error ,  fail message = {}", JobUtil.getErrorMsg(e));
                XxlJobLogger.log("delete storage clickhouse error ,  fail message = {}", JobUtil.getErrorMsg(e));
                failCount++;
            }

            modifyLastStorage(Constant.ZK_TRAFFIC_LOGS, 0);
            if (failCount > 0) {
                throw new BusinessException("delete storage clickhouse error ,failCount " + failCount);
            }
            logger.info("delete clickhouse success");
            XxlJobLogger.log("delete clickhouse success");
        } catch (BusinessException be) {
            logger.error(be.getMessage());
            XxlJobLogger.log(be.getMessage());
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

            String deleteendpoint = UrlUtil.getUrl(deletionConfig.getAnalyticServer());

            String deleteStartTime = getDruidDataStartTime();
            //1停止加载
            String changeRulesUrl = Joiner.on("").join(deleteendpoint, "/druid/coordinator/v1/rules/");
            String changeRulesParam = "[{ \"type\":\"dropByInterval\", \"interval\":\"" + deleteStartTime + "/" + deleteEndDate + "\" }]";

            //2标记unused
            String unUseParam = Joiner.on("").join("{\"interval\":\"", deleteStartTime, "/", deleteEndDate, "\"}");
            JobUtil.infoLog("reset storage days druid unused command = {}", unUseParam);
            //3物理删除所有数据
            String deleteUrl = Joiner.on("").join(deleteendpoint, "/druid/indexer/v1/task");
            String prefixDeleteParam = "{\"type\":\"kill\",\"dataSource\":\"";
            String suffixDeleteParam = Joiner.on("").join("\",\"interval\":\"", deleteStartTime + "/", deleteEndDate, "\"}");
            //4恢复加载规则
            String resetRulesParam = Joiner.on("").join("[{\"type\":\"loadByPeriod\",\"period\":\"P", paramsMap.get(max_days), "D\",\"tieredReplicants\":{\"_default_tier\":1}},{\"type\":\"dropForever\"}]");
            //清库时注意不操作hot结尾的表
            String queryTablesSql = "{\"query\":\"SELECT DISTINCT(datasource) FROM sys.tasks WHERE datasource NOT LIKE '%hot%' " + deletionConfig.getNotInSql("datasource", deletionConfig.getAnalyticExclusion()) + " \",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";

            List<String> analyticTables = getAnalyticTables(UrlUtil.getUrl(deletionConfig.getAnalyticServer(), analyticUrl), queryTablesSql);
            int failCount = 0;
            for (String table : analyticTables) {
                try {
                    //每个表限制操作时间五分钟，开始计时
                    long startTime = System.currentTimeMillis();
                    long endTime = startTime + 1000 * 60 * 5;
                    XxlJobLogger.log("delete druid table first command , endpoint = {}, table = {}, sql = {}", deletionConfig.getAnalyticServer(), table, changeRulesParam);
                    HttpClientUtils.httpPost(changeRulesUrl + table, changeRulesParam);

                    //查询表数据是否删除 固定key EXPR$0
                    String countTableDataSql = Joiner.on("").join("{\"query\":\"SELECT count(*) from ", table, " where __time >= '", deleteStartTime, "' AND __time <= '", getDruidDataEndTime(UrlUtil.getUrl(deletionConfig.getAnalyticServer(), analyticUrl)), "'\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}");
                    int count = 1;
                    while (count > 0) {
                        String httpPostResult = HttpClientUtils.httpPost(UrlUtil.getUrl(deletionConfig.getAnalyticServer(), analyticUrl), countTableDataSql);
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

                    String unUseMessage = HttpClientUtils.httpPost(UrlUtil.getUrl(deletionConfig.getAnalyticServer(), "/druid/coordinator/v1/datasources/", table,"/markUnused") , unUseParam);
                    JobUtil.infoLog("reset storage days druid unused result message :{}", unUseMessage);

                    XxlJobLogger.log("delete druid table second command table = {}, sql = {}", table, prefixDeleteParam + table + suffixDeleteParam);
                    String deleteMessage = HttpClientUtils.httpPost(deleteUrl, prefixDeleteParam + table + suffixDeleteParam);
                    XxlJobLogger.log("delete druid table third command table = {}, sql = {}", table, resetRulesParam);
                    HttpClientUtils.httpPost(changeRulesUrl + table, resetRulesParam);

                    XxlJobLogger.log("delete druid table result message = {}", deleteMessage);
                } catch (BusinessException e) {
                    // 表不存在时，查询发生错误，记录下继续执行
                    logger.error(" query druid error, table = {}, fail message = {}", table, JobUtil.getErrorMsg(e));
                    XxlJobLogger.log(" query druid error, endpoint = {}, table = {}, fail message = {}", deletionConfig.getAnalyticServer(), table, JobUtil.getErrorMsg(e));
                    failCount++;
                }
            }
            modifyLastStorage(Constant.ZK_REPORT_AND_METRICS, 0);
            if (failCount > 0) {
                throw new BusinessException("delete druid error, fail count " + failCount);
            }
        } catch (BusinessException be) {
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
            Map<String, Object> paramsMap = validParams(params);
            if (ObjectUtils.isEmpty(paramsMap)) {
                logger.error("params parser error , params is {}", params);
                return IJobHandler.FAIL;
            }
            Header header = new BasicHeader(Constant.TOKEN, deletionConfig.getFilesToken());
            List<String> endpointList = Splitter.on(",").trimResults().omitEmptyStrings().splitToList(deletionConfig.getFilesHosServer());
            List<String> whiteList = StringUtil.isEmpty(deletionConfig.getFilesExclusion()) ? null : Splitter.on(",").trimResults().omitEmptyStrings().splitToList(deletionConfig.getFilesExclusion());

            int failCount = 0;
            for (String endpoint : endpointList) {
                try {
                    String filesServer = UrlUtil.getUrl(endpoint);
                    String httpGetRes = HttpClientUtils.httpGet(Joiner.on("").join(filesServer, "/hos/"), header);

                    if (httpGetRes != null && !"-1".equals(httpGetRes)) {
                        ListAllMyBucketsResult list = XmlUtil.converXmlToBean(ListAllMyBucketsResult.class, httpGetRes);
                        logger.info("delete storage files ttl endpoint = {}, bucket list = {} ", endpoint, list);
                        XxlJobLogger.log("endpoint = {}, bucket list = {} ", endpoint, list);
                        if (ObjectUtils.allNotNull(list, list.getBuckets(), list.getBuckets().getBucket())) {
                            ListAllMyBucketsResult.Buckets buckets = list.getBuckets();
                            List<ListAllMyBucketsResult.Buckets.Bucket> bucketList = buckets.getBucket();
                            XxlJobLogger.log("delete file endpoint = {}, bucket list = {} ", endpoint, bucketList);
                            logger.info("delete file endpoint = {}, bucket list = {} ", endpoint, bucketList);
                            for (ListAllMyBucketsResult.Buckets.Bucket bucket : bucketList) {
                                try {
                                    //排除白名单
                                    if (ObjectUtils.isEmpty(whiteList) || !whiteList.contains(bucket.getName())) {
                                        HttpClientUtils.httpDelete(Joiner.on("").join(filesServer, "/hos/", bucket.getName(), "?truncate"), header);
                                    }
                                } catch (BusinessException e) {
                                    failCount++;
                                    logger.error("delete file error endpoint = {},bucket ={}, fail message={}", endpoint, bucket, JobUtil.getErrorMsg(e));
                                    XxlJobLogger.log("endpoint = {},bucket ={}, fail message={}", endpoint, bucket, JobUtil.getErrorMsg(e));
                                }
                            }
                        }
                    } else {
                        failCount++;
                        logger.error("delete file error failCount = {}, endpoint = {} , fail message = I/O exception", failCount, endpoint);
                        XxlJobLogger.log("endpoint = {} , fail message = I/O exception", endpoint);
                    }
                } catch (BusinessException e) {
                    failCount++;
                    logger.error("delete file error endpoint = {}, fail message={}", endpoint, JobUtil.getErrorMsg(e));
                    XxlJobLogger.log("endpoint = {}, fail message={}", endpoint, JobUtil.getErrorMsg(e));

                }
            }
            modifyLastStorage(Constant.FILES, 0);
            if (failCount > 0) {
                throw new BusinessException(" delete files error, failed count " + failCount);
            }
        } catch (BusinessException be) {
            logger.error(be.getMessage());
            XxlJobLogger.log(be.getMessage());
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

    /**
     * 查询clickhouse 需要删除的表
     *
     * @return List<String>
     */
    private List<String> getTablesForCK(String url, String condition) {
        return getSystemDataForCK(url, Joiner.on("").join("SELECT DISTINCT(name) FROM ", deletionConfig.getSystemTables(), " WHERE database = '", deletionConfig.getTrafficDatasource(), "' AND engine in  ('MergeTree','ReplicatedMergeTree')", condition == null ? "" : condition, " FORMAT JSON;"));
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
        return ObjectUtils.isEmpty(startTimeList) ? DateUtils.getCurrentDate() : DateUtils.getFormatDate(DateUtils.convertStringToDate(String.valueOf(startTimeList.get(0).get("start")), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"), DateUtils.YYYY_MM_DD);
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
