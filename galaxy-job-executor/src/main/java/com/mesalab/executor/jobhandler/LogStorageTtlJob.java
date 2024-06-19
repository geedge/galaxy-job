package com.mesalab.executor.jobhandler;

import cn.hutool.core.net.url.UrlBuilder;
import cn.hutool.log.Log;
import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import com.geedgenetworks.utils.JsonMapper;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.*;


@Component
public class LogStorageTtlJob {

    private Log logger = Log.get();

    private static final String trafficPort = "8123";
    private static final int materializeTtlAfterModify = 0;

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
    @XxlJob("changeCkTtlJobHandler")
    public ReturnT<String> changeCkTtl(String params) {
        try {
             // deletionConfig.setTrafficServer("192.168.40.203:8124");
           UrlBuilder ckurlBuilder = UrlBuilder.ofHttp(deletionConfig.getTrafficServer(), StandardCharsets.UTF_8);

            if (ifDdlQueueEmpty(ckurlBuilder.toString())) {
            //    if (true) {
                List<String> addressForCKs = getAddressForCK(ckurlBuilder.toString());
                List<String> tablesForCKs = getTablesForCK(ckurlBuilder.toString(), deletionConfig.getNotInSql("name", null));
                Map<String, Map<String, String>> getChangeTtlTables = getChangeTtlTable(tablesForCKs, addressForCKs);
                modifyTableTtl(ckurlBuilder, getChangeTtlTables);

                for (String table : tablesForCKs) {

                    Map<String, Map<String, ColumnParam>> getChangeTtlColumns = getChangeTtlColumns(table, addressForCKs);
                    if (getChangeTtlColumns == null) {
                        logger.info("ChangeTtlColumns "+table+" is error");
                        XxlJobLogger.log("ChangeTtlColumns "+table+" is error");
                        continue;
                    }
                    modifyColumnTtl(ckurlBuilder, getChangeTtlColumns, table);
                }

            } else {
                logger.info("ddl queue  busy ");
                XxlJobLogger.log("ddl queue  busy ");
                return ReturnT.FAIL;
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

            if (modifyTtlTables != null && modifyTtlTables.size() > 0) {


                for (Map.Entry<String, String> entry : modifyTtlTables.entrySet()) {

                    try {
                        StringBuilder query = new StringBuilder("alter table  " + deletionConfig.getTrafficDatasource() + "." + entry.getKey() + " on cluster ck_cluster MODIFY TTL " + entry.getValue());
                        alterTablesForCK(ckurlBuilder.toString(), query.toString());
                        XxlJobLogger.log("table ddl sql : " + query.toString());
                    } catch (RuntimeException be) {

                        XxlJobLogger.log("modify table " + entry.getKey() + "ddl sql error: " + be.toString());

                    }
                }
            }

            if (dropTtlTables != null && dropTtlTables.size() > 0) {

                for (Map.Entry<String, String> entry : dropTtlTables.entrySet()) {

                    try {
                        StringBuilder sqery = new StringBuilder("alter table " + deletionConfig.getTrafficDatasource() + "." + entry.getKey() + " on cluster ck_cluster REMOVE TTL");
                        alterTablesForCK(ckurlBuilder.toString(), sqery.toString());
                        XxlJobLogger.log("table ddl sql : " + sqery.toString());
                    } catch (RuntimeException be) {

                        XxlJobLogger.log("drop table " + entry.getKey() + "ddl sql error: " + be.toString());

                    }
                }
            }

        } catch (RuntimeException be) {

            logger.error("modifyTableTtl function error " + be.getMessage());
            XxlJobLogger.log(be.getMessage());
        }
        return true;
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
                XxlJobLogger.log(table + " column ttl no change");
            }
        } catch (RuntimeException be) {

            logger.error("modifyColumnTtl function error " + be.getMessage());
            XxlJobLogger.log(be.getMessage());
        }
        return true;
    }

    private Map<String, Object> getDeleteSource() {
        //删除sql参数拼接
        Map<String, Object> deleteParamMap = Maps.newHashMap();
        deleteParamMap.put("database", deletionConfig.getTrafficDatasource());
        deleteParamMap.put("password", deletionConfig.getTrafficUserKey());
        deleteParamMap.put("user", deletionConfig.getTrafficUsername());
        return deleteParamMap;
    }


    private Map<String, Map<String, String>> getChangeTtlTable(List<String> tablesForCKs, List<String> addressForCKs) {
        Map<String, Map<String, String>> ChangeTtltables = new HashMap<>();

        try {

            Map<String, String> modifyTtltables = new HashMap<>();
            Map<String, String> dropTtltables = new HashMap<>();
            Configuration conf = Configuration.defaultConfiguration();
            Configuration conf2 = conf.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);

            for (String tablename : tablesForCKs) {
                try {
                    String viewName = tablename.replace("_local", "");
                    String content = configService.getConfig(viewName + ".json", "Galaxy", 3000);

                    if (content != null && !"".equals(content)) {

                        String getTableDdlsql = "SELECT name ,extract(engine_full, '.*toIntervalSecond\\((\\d+)\\)') as tablettl  from `system`.tables where name ='" + tablename + "' and  database = '" + deletionConfig.getTrafficDatasource() + "' and engine ='MergeTree'  format JSON";

                        String databaseschema = "$..data[*]";
                        String ttltime = null;
                        String partitionKey = null;
                        Map<String, Object> schemaobj = (Map) JsonMapper.fromJsonString(content, Map.class);

                        if (schemaobj.containsKey("doc")) {
                            Map<String, Object> schemaHasDoc = (Map<String, Object>) schemaobj.get("doc");
                            if (schemaHasDoc.containsKey("partition_key") && schemaHasDoc.containsKey("ttl")) {

                                if (schemaHasDoc.get("ttl")!=null && schemaHasDoc.get("partition_key")!=null) {
                                    ttltime = schemaHasDoc.get("ttl").toString().trim();
                                    partitionKey = schemaHasDoc.get("partition_key").toString().trim();
                                } else {


                                }
                            } else {

                                logger.warn("table " + tablename + " schema  not containsKey partition_key or ttl error ");
                                XxlJobLogger.log("table " + tablename + " schema  not containsKey partition_key or ttl error ");
                            }
                        } else {
                            logger.warn("table " + tablename + " schema  not containsKey doc  error ");
                            XxlJobLogger.log("table " + tablename + " schema  not containsKey doc  error ");

                        }


                        for (String ip : addressForCKs) {
                            try {
                                String tableDdl = gettableDdl(ip, getTableDdlsql);

                                List<Map<String, Object>> databaseMessageList = JsonPath.using(conf2).parse(tableDdl).read(databaseschema, List.class);

                                if (databaseMessageList.size() > 0) {
                                    String tTtl = databaseMessageList.get(0).get("tablettl").toString().trim();


                                    if (ttltime != null) {
                                        if (!ttltime.equals(tTtl)) {

                                            String ttlvalue = "toDateTime(" + partitionKey + ") + toIntervalSecond(" + ttltime + ")";
                                            modifyTtltables.put(tablename, ttlvalue);
                                            XxlJobLogger.log("TableTTl is different ! ip:" + ip + " tablename: " + tablename + " TTL :" + tTtl + " schematableTTL :" + ttltime + " modify table TTL!");
                                        }
                                    } else {

                                        if (!"".equals(tTtl)) {
                                            dropTtltables.put(tablename, "");
                                            XxlJobLogger.log("TableTTl is different ! ip:" + ip + " tablename: " + tablename + " TTL :" + tTtl + " schematableTTL :" + ttltime + " drop table TTL!");

                                        }


                                    }

                                }

                            } catch (RuntimeException be) {

                                logger.error("table " + tablename + " foreach  local ip " + ip + " error " + be.getMessage());
                                XxlJobLogger.log(be.getMessage());

                            }
                        }

                    } else {
                        XxlJobLogger.log(tablename + " table is not exist in schema !");
                    }

                } catch (RuntimeException be) {
                    logger.error(" getChangeTtltables foreach table " + tablename + "  error " + be.getMessage());
                    XxlJobLogger.log(" getChangeTtltables foreach table " + tablename + "  error " + be.getMessage());

                }
            }

            ChangeTtltables.put("modifyTtlTables", modifyTtltables);
            ChangeTtltables.put("dropTtlTables", dropTtltables);
            return ChangeTtltables;
        } catch (RuntimeException | NacosException be) {

            logger.error(" getChangeTtltables function error " + be.getMessage());
            XxlJobLogger.log(" getChangeTtltables function error " + be.getMessage());
        }

        return ChangeTtltables;
    }

    private String gettableDdl(String ip, String getTableDdlsql) {

        Map<String, Object> deleteParamMap = getDeleteSource();
        String httpResult = HttpClientUtils.httpPost(HttpClientUtils.getUrlWithParams("http://" + ip, deleteParamMap), getTableDdlsql);
        if ("-1".equals(httpResult)) {
            throw new BusinessException("http request error url = " + ip);
        }
        return httpResult;
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
            Map<String, Map<String, ColumnParam>> ChangeTtlColumns = new HashMap<>();
            Map<String, ColumnParam> modifyTtlColumns = new HashMap<>();
            Map<String, ColumnParam> dropTtlColumns = new HashMap<>();
            String viewName = tablename.replace("_local", "");
            String content = configService.getConfig(viewName + ".json", "Galaxy", 3000);
            String partitionKey = "";
            Configuration conf = Configuration.defaultConfiguration();
            Configuration conf2 = conf.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
            String getdatabaseDDL = "desc " + deletionConfig.getTrafficDatasource() + "." + tablename + " format JSON";
            String schema = "$..fields[*]";
            String databaseschema = "$['data']";
            Map<String, Object> deleteParamMap = getDeleteSource();
            List<Map<String, Object>> schemaColumnMessageList = null;
            if (content != null && !"".equals(content)) {


                Map<String, Object> schemaobj = (Map) JSON.parse(content);


                if (schemaobj.containsKey("doc")) {
                    Map<String, Object> schemaHasDoc = (Map<String, Object>) schemaobj.get("doc");
                    partitionKey = (String) schemaHasDoc.get("partition_key");
                    if (schemaHasDoc.containsKey("partition_key")) {
                        schemaColumnMessageList = JsonPath.using(conf2).parse(content).read(schema, List.class);

                    } else {
                        logger.warn("table " + tablename + " schema  not containsKey partition_key  error ");
                        XxlJobLogger.log("table " + tablename + " schema  not containsKey partition_key  error ");
                        return null;
                    }
                }else {

                    logger.warn("table " + tablename + " schema  not containsKey doc  error ");
                    XxlJobLogger.log("table " + tablename + " schema  not containsKey doc  error ");
                    return null;
                }
            } else {
                logger.warn("table " + tablename + " schema  not  exist ");
                XxlJobLogger.log(tablename + " table is not exist in schema !");
                return null;
            }

            for (String ip : addressForCKs) {
                try {
                    String databaseDdl = HttpClientUtils.httpPost(HttpClientUtils.getUrlWithParams("http://" + ip, deleteParamMap), getdatabaseDDL);


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
                                if(ttltime!=null) {
                                    String ttlvalue = "toDateTime(" + partitionKey + ") + toIntervalSecond(" + ttltime + ")";
                                    if (!ttlvalue.equals(tableobj.get(column).getTtl().trim())) {
                                        tableobj.get(column).setTtl(ttlvalue);
                                        modifyTtlColumns.put(column, tableobj.get(column));
                                        //  XxlJobLogger.log("ChangeTtlColumns error! ip:"+ip+" tablename :"+tablename+" different ttl columns: "+be.getMessage());

                                    }
                                }
                                else {
                                    if (tableobj.containsKey(column)) {
                                        if (tableobj.get(column).getTtl() != null && !("").equals(tableobj.get(column).getTtl())) {
                                            tableobj.get(column).setTtl("");
                                            dropTtlColumns.put(column, tableobj.get(column));

                                        }
                                    }

                                }

                            } else {

                                if (tableobj.containsKey(column)) {
                                    if (tableobj.get(column).getTtl() != null && !("").equals(tableobj.get(column).getTtl())) {
                                        tableobj.get(column).setTtl("");
                                        dropTtlColumns.put(column, tableobj.get(column));

                                    }
                                }
                            }
                        } else {
                            if (tableobj.containsKey(column)) {

                                if (tableobj.get(column).getTtl() != null && !("").equals(tableobj.get(column).getTtl())) {
                                    tableobj.get(column).setTtl("");
                                    dropTtlColumns.put(column, tableobj.get(column));

                                }
                            }
                        }

                    }
                } catch (RuntimeException be) {

                    logger.error("getChangeTtlColumns error! ip:" + ip + " tablename :" + tablename + " message: " + be.getMessage());
                    XxlJobLogger.log("getChangeTtlColumns error! ip:" + ip + " tablename :" + tablename + " message: " + be.getMessage());

                }
            }


            ChangeTtlColumns.put("modifyTtlColumns", modifyTtlColumns);
            ChangeTtlColumns.put("dropTtlColumns", dropTtlColumns);
            return ChangeTtlColumns;

        } catch (RuntimeException | NacosException be) {
            logger.error("getChangeTtlColumns function error " + be.getMessage());
            XxlJobLogger.log("getChangeTtlColumns function error " + be.getMessage());
        }

        return null;
    }


    private Boolean alterTablesForCK(String url, String sql) {

        return excutesqlToCK(url, sql+" SETTINGS materialize_ttl_after_modify = "+materializeTtlAfterModify);
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

        String sql = "select query from system.distributed_ddl_queue_cluster where status =0 limit 1 format JSON";
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


}
