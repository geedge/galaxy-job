package com.mesalab.executor.jobhandler;

import cn.hutool.core.net.url.UrlBuilder;
import cn.hutool.log.Log;
import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.mesalab.executor.core.config.StorgeConfig;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.core.utils.SpringContextUtil;
import com.mesalab.executor.exception.BusinessException;
import com.mesalab.executor.pojo.ColumnParam;
import com.mesalab.executor.service.StorageQuotaService;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.*;


@Component
public class LogStorageTtlJob {

    private Log logger = Log.get();

    private static final String trafficPort = "8123";

    @Autowired
    StorageQuotaService storageQuotaService;
    @Autowired
    ConfigService configService;

    private static StorgeConfig deletionConfig = (StorgeConfig) SpringContextUtil.getBean("storgeConfig");

    //*********************************************设置存储策略**********************************************************


    /**
     * 设置流量日志存储策略储
     * clickhouse 数据库
     *
     * @param params {"maxdays":30}
     */
    @XxlJob("changeColumnTtlJobHandler")
    public ReturnT<String> changeColumnTtl(String params) {
        try {

            UrlBuilder ckurlBuilder = UrlBuilder.ofHttp(deletionConfig.getTrafficServer(), StandardCharsets.UTF_8);

            if (ifDdlQueueEmpty(ckurlBuilder.toString())) {

                List<String> addressForCKs = getAddressForCK(ckurlBuilder.toString());
                List<String> tablesForCKs = getTablesForCK(ckurlBuilder.toString(), deletionConfig.getNotInSql("name", null));
                Map<String, Map<String, String>> getChangeTtlTables = getChangeTtlTable(tablesForCKs, addressForCKs);
                modifyTableTtl(ckurlBuilder, getChangeTtlTables);

                for (String table : tablesForCKs) {

                    Map<String, Map<String, ColumnParam>> getChangeTtlColumns = getChangeTtlColumns(table, addressForCKs);
                    if (getChangeTtlColumns == null) {
                        logger.info("ChangeTtlColumns is empty");
                        continue;
                    }
                    modifyColumnTtl(ckurlBuilder, getChangeTtlColumns, table);
                }

            } else {
                logger.info("ddl queue  busy ");
            }


        } catch (RuntimeException be) {
            logger.error(be.getMessage());
            XxlJobLogger.log(be.getMessage());
            return ReturnT.FAIL;
        }
        return ReturnT.SUCCESS;
    }


    private Boolean modifyTableTtl(UrlBuilder ckurlBuilder, Map<String, Map<String, String>> changeTtlTables) {

        try {
        Map<String, String> modifyTtlTables = changeTtlTables.get("modifyTtlTables");
        Map<String, String> dropTtlTables = changeTtlTables.get("dropTtlTables");

        if (modifyTtlTables!=null && modifyTtlTables.size() > 0 ) {


            for (Map.Entry<String, String> entry : modifyTtlTables.entrySet()) {

                System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
                StringBuilder sqery = new StringBuilder("alter table  " + deletionConfig.getTrafficDatasource() + "." + entry.getKey() + " on cluster ck_cluster MODIFY TTL " + entry.getValue());
                alterTablesForCK(ckurlBuilder.toString(), sqery.toString());
                XxlJobLogger.log("table ddl sql : " + sqery.toString());

            }
        }

            if (dropTtlTables!=null && dropTtlTables.size() > 0) {
                for (Map.Entry<String, String> entry : dropTtlTables.entrySet()) {

                    System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
                    StringBuilder sqery = new StringBuilder("alter table " + deletionConfig.getTrafficDatasource() + "." + entry.getKey() + " on cluster ck_cluster REMOVE TTL");
                    alterTablesForCK(ckurlBuilder.toString(), sqery.toString());
                    XxlJobLogger.log("table ddl sql : " + sqery.toString());

                }
            }

        }
        catch (RuntimeException be) {

                logger.error("modifyTableTtl function error " + be.getMessage());
                XxlJobLogger.log(be.getMessage());
            }
            return  true;
        }

    private Boolean modifyColumnTtl(UrlBuilder ckurlBuilder, Map<String, Map<String, ColumnParam>> getChangeTtlColumns, String table) {


        try {

        Map<String, ColumnParam> modifyTtlColumns = getChangeTtlColumns.get("modifyTtlColumns");
        Map<String, ColumnParam> dropTtlColumns = getChangeTtlColumns.get("dropTtlColumns");


        if (modifyTtlColumns.size() > 0 || dropTtlColumns.size() > 0) {
            StringBuilder sqery = new StringBuilder("alter table " + deletionConfig.getTrafficDatasource() + "." + table + " on cluster ck_cluster ");

            for (Map.Entry<String, ColumnParam> entry : modifyTtlColumns.entrySet()) {
                sqery.append("MODIFY COLUMN if EXISTS  ");
                sqery.append(entry.getValue().getName());
                sqery.append(" ");
                sqery.append(entry.getValue().getType());
                sqery.append(" TTL ");
                sqery.append(entry.getValue().getTtl());
                sqery.append(",");
            }

            for (Map.Entry<String, ColumnParam> entry : dropTtlColumns.entrySet()) {
                sqery.append("MODIFY COLUMN if EXISTS  ");
                sqery.append(entry.getValue().getName());
                sqery.append(" ");
                sqery.append("REMOVE TTL");
                sqery.append(",");

            }
            sqery.deleteCharAt(sqery.length() - 1);
            String tquery = sqery.toString();
            logger.info("ddl sql : " + tquery);
            XxlJobLogger.log("ddl sql : " + tquery);
            alterTablesForCK(ckurlBuilder.toString(), tquery);

        } else {
            logger.info(table + "column ttl no change");
            XxlJobLogger.log(table + "column ttl no change");
        }
    }
        catch (RuntimeException be) {

            logger.error("modifyColumnTtl function error " + be.getMessage());
            XxlJobLogger.log(be.getMessage());
        }
       return  true;
    }

    private Map<String, Object> getDeleteSource() {
        //删除sql参数拼接
        Map<String, Object> deleteParamMap = Maps.newHashMap();
        deleteParamMap.put("database", deletionConfig.getTrafficDatasource());
        deleteParamMap.put("password", deletionConfig.getTrafficUserKey());
        deleteParamMap.put("user", deletionConfig.getTrafficUsername());
      //  deleteParamMap.put("password", "galaxy2019");
        return deleteParamMap;
    }


    private Map<String, Map<String, String>> getChangeTtlTable(List<String> tablesForCKs, List<String> addressForCKs) {

        try {

            Map<String, Map<String, String>> ChangeTtlColumns = new HashMap<>();
            Map<String, String> modifyTtlColumns = new HashMap<>();
            Map<String, String> dropTtlColumns = new HashMap<>();
            Map<String, Object> deleteParamMap = getDeleteSource();
            Configuration conf = Configuration.defaultConfiguration();
            Configuration conf2 = conf.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);

            for (String tablename : tablesForCKs) {

                String viewName = tablename.replace("_local", "");
                String content = configService.getConfig(viewName + ".json", "Galaxy", 3000);

                if (content != null && !"".equals(content)) {

                    String getdatabaseDDL = "SELECT name ,extract(engine_full, '.*toIntervalSecond\\((\\d+)\\)') as tablettl  from `system`.tables where tablettl!='' and  database = '" + deletionConfig.getTrafficDatasource() + "' and engine ='MergeTree' and engine_full like '%TTL%' format JSON";

                    String databaseschema = "$..data[*]";
                    // String tableschema = "$.[data]";

                    //  List<Map<String, Object>> schemaTableMessageList = JsonPath.using(conf2).parse(content).read(tableschema, List.class);

                    for (String ip : addressForCKs) {
                        String databaseDdl = HttpClientUtils.httpPost(HttpClientUtils.getUrlWithParams("http://" + ip, deleteParamMap), getdatabaseDDL);
                        List<Map<String, Object>> databaseMessageList = JsonPath.using(conf2).parse(databaseDdl).read(databaseschema, List.class);
                        Map<String, Integer> tableobj = new HashMap<>();
                        for (Map<String, Object> obj : databaseMessageList) {
                            String tName = (String) obj.get("name");
                            Integer tTtl = Integer.valueOf((String) obj.get("tablettl"));
                            tableobj.put(tName, tTtl);

                        }
                        Map<String, Object> schemaobj = (Map) JSON.parse(content);

                        if (schemaobj.containsKey("doc")) {
                            Map<String, Object> schemaColumnHasDoc = (Map<String, Object>) schemaobj.get("doc");
                            if (schemaColumnHasDoc.containsKey("ttl") && schemaColumnHasDoc.containsKey("partition_key")) {

                                Integer ttltime = (Integer) schemaColumnHasDoc.get("ttl");
                                String partitionKey = (String) schemaColumnHasDoc.get("partition_key");

                                if (tableobj.containsKey(tablename)) {
                                    if (!ttltime.equals(tableobj.get(tablename))) {
                                        String ttlvalue = "toDateTime(" + partitionKey + ") + toIntervalSecond(" + ttltime + ")";
                                        modifyTtlColumns.put(tablename, ttlvalue);
                                    }

                                } else {
                                    String ttlvalue = "toDateTime(" + partitionKey + ") + toIntervalSecond(" + ttltime + ")";
                                    modifyTtlColumns.put(tablename, ttlvalue);

                                }


                            } else {

                                if (tableobj.containsKey(tablename)) {
                                    dropTtlColumns.put(tablename, "");

                                }
                            }
                        } else {

                        }


                    }

                } else {
                    XxlJobLogger.log(tablename + " table is not exist in schema !");
                }


            }
            ChangeTtlColumns.put("modifyTtlTables", modifyTtlColumns);
            ChangeTtlColumns.put("dropTtlTables", dropTtlColumns);
            return ChangeTtlColumns;
        } catch (RuntimeException | NacosException be) {

            logger.error(" getChangeTtlColumns function error " + be.getMessage());
            XxlJobLogger.log(be.getMessage());
        }

        return null;
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
    private Map<String, Map<String, ColumnParam>> getChangeTtlColumns(String tablename, List<String> addressForCKs) {

        try {


            String viewName = tablename.replace("_local", "");
            String content = configService.getConfig(viewName + ".json", "Galaxy", 3000);
            if (content != null && !"".equals(content)) {

                Map<String, Map<String, ColumnParam>> ChangeTtlColumns = new HashMap<>();

                Configuration conf = Configuration.defaultConfiguration();

                //   List<Object> revenueList = context.read("$[*]['box office']");
                Configuration conf2 = conf.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
                String getdatabaseDDL = "desc " + deletionConfig.getTrafficDatasource() + "." + tablename + " format JSON";

                Map<String, Object> deleteParamMap = getDeleteSource();

                String schema = "$..fields[*]";
                String databaseschema = "$['data']";

                Map<String, ColumnParam> modifyTtlColumns = new HashMap<>();
                Map<String, ColumnParam> dropTtlColumns = new HashMap<>();

                for (String ip : addressForCKs) {
                    String databaseDdl = HttpClientUtils.httpPost(HttpClientUtils.getUrlWithParams("http://" + ip, deleteParamMap), getdatabaseDDL);


                    List<Map<String, Object>> schemaColumnMessageList = JsonPath.using(conf2).parse(content).read(schema, List.class);
                    List<Map<String, Object>> databaseColumnMessageList = JsonPath.using(conf2).parse(databaseDdl).read(databaseschema, List.class);
                    Map<String, ColumnParam> tableobj = new HashMap<>();
                    for (Map<String, Object> obj : databaseColumnMessageList) {
                        ColumnParam columnParam = new ColumnParam();
                        columnParam.setName((String) obj.get("name"));
                        columnParam.setType((String) obj.get("type"));
                        columnParam.setTtl((String) obj.get("ttl_expression"));
                        tableobj.put((String) obj.get("name"), columnParam);

                    }


                    for (Map<String, Object> colobj : schemaColumnMessageList) {

                        String column = (String) colobj.get("name");


                        if (colobj.containsKey("doc")) {
                            Map<String, Object> schemaColumnHasDoc = (Map<String, Object>) colobj.get("doc");
                            if (schemaColumnHasDoc.containsKey("ttl")) {

                                Integer ttltime = (Integer) schemaColumnHasDoc.get("ttl");
                                String ttlvalue = "toDateTime(common_recv_time) + toIntervalSecond(" + ttltime + ")";
                                if (!ttlvalue.equals(tableobj.get(column).getTtl())) {
                                    tableobj.get(column).setTtl(ttlvalue);
                                    modifyTtlColumns.put(column, tableobj.get(column));
                                }
                            } else {

                                if(tableobj.containsKey(column)) {
                                    if (tableobj.get(column).getTtl() != null && !("").equals(tableobj.get(column).getTtl())) {
                                        tableobj.get(column).setTtl("");
                                        dropTtlColumns.put(column, tableobj.get(column));

                                    }
                                }
                            }
                        } else {
                            if(tableobj.containsKey(column)) {

                                if (tableobj.get(column).getTtl() != null && !("").equals(tableobj.get(column).getTtl())) {
                                    tableobj.get(column).setTtl("");
                                    dropTtlColumns.put(column, tableobj.get(column));

                                }
                            }
                        }

                    }
                }
                ChangeTtlColumns.put("modifyTtlColumns", modifyTtlColumns);
                ChangeTtlColumns.put("dropTtlColumns", dropTtlColumns);
                return ChangeTtlColumns;
            } else {
                XxlJobLogger.log(tablename + " table is not exist in schema !");
                return null;
            }
        } catch (RuntimeException | NacosException be) {
            be.printStackTrace();
            logger.error("getChangeTtlColumns function error " + be.getMessage());
            XxlJobLogger.log(be.getMessage());
        }

        return null;
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

    private Boolean alterTablesForCK(String url, String sql) {

        //   sql = sql.replaceAll("\\+", "%2B");
        return excutesqlToCK(url, sql);
    }

    /***
     * --查所有的IP地址
     * @Date 2020\9\18 0018
     * @return java.util.List<java.lang.String>
     **/
    private List<String> getAddressForCK(String url) {
        List<String> endpointList = new ArrayList<>();
        endpointList.addAll(getSystemDataForCK(url, Joiner.on("").join("SELECT DISTINCT concat(host_address,':','" + trafficPort + "') as endpoint FROM ", deletionConfig.getSystemClusters(), " where cluster = 'ck_cluster' FORMAT JSON;")));
        return endpointList;
    }


    private Boolean ifDdlQueueEmpty(String url) {
        Map<String, Object> deleteParamMap = getDeleteSource();
        Configuration conf = Configuration.defaultConfiguration();
        Configuration conf2 = conf.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);

        String sql = "select query,(toUnixTimestamp(now())-toUnixTimestamp(query_start_time)) as duration_time from system.distributed_ddl_queue where status =0 limit 1 format JSON";
        String httpResult = HttpClientUtils.httpPost(HttpClientUtils.getUrlWithParams(url, deleteParamMap), sql);
        if ("-1".equals(httpResult)) {
            throw new BusinessException("http request error url = " + url);
        }

        String databaseschema = "$..data[*]";
        List<Map<String, Object>> databaseMessageList = JsonPath.using(conf2).parse(httpResult).read(databaseschema, List.class);

        if (databaseMessageList.size() == 0) {

            return true;
        } else {

            for (Map<String, Object> mapobj : databaseMessageList) {

                logger.warn("zk queue is excuting sql:  " + mapobj.get("query") + " duration_time:" + mapobj.get("duration_time"));
                XxlJobLogger.log("zk queue is excuting sql:  " + mapobj.get("query") + " duration_time:" + mapobj.get("duration_time"));

            }

            return false;
        }
    }

    /**
     * 查询clickhouse 需要删除的表
     *
     * @return List<String>
     */
    private List<String> getTablesForCK(String url, String condition) {
        return getSystemDataForCK(url, Joiner.on("").join("SELECT DISTINCT(name) FROM ", "`system`.tables_cluster", " WHERE database = '", deletionConfig.getTrafficDatasource(), "' AND engine in  ('MergeTree','ReplicatedMergeTree')", condition == null ? "" : condition, " FORMAT JSON;"));
    }

    //公共查询clickhouse
    private List<String> getSystemDataForCK(String url, String sql) {
        List<String> dataList = Lists.newArrayList();
        Map<String, Object> deleteParamMap = getDeleteSource();
        //deleteParamMap.put("query", sql);
        String httpGetResult = HttpClientUtils.httpPost(HttpClientUtils.getUrlWithParams(url, deleteParamMap), sql);

        if ("-1".equals(httpGetResult)) {
            throw new BusinessException("http request error url = " + url);
        }
        Map httpGetResultMap = (Map) JsonMapper.fromJsonString(httpGetResult, Map.class);
        List<Map> list = (List) httpGetResultMap.get("data");
        list.stream().filter(Objects::nonNull).forEach(map -> dataList.addAll(map.values()));
        return dataList;
    }

    private Boolean excutesqlToCK(String url, String sql) {
        List<String> dataList = Lists.newArrayList();
        Map<String, Object> deleteParamMap = getDeleteSource();
        String httpResult = HttpClientUtils.httpPost(HttpClientUtils.getUrlWithParams(url, deleteParamMap), sql);

        if ("-1".equals(httpResult)) {
            throw new BusinessException("http request error url = " + url);
        }

        return true;
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


}
