package com.mesalab.executor.test;

import cn.hutool.core.date.DateUtil;
import cn.hutool.json.JSONUtil;
import com.geedgenetworks.utils.DateUtils;
import com.geedgenetworks.utils.JsonMapper;
import com.geedgenetworks.utils.StringUtil;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.mesalab.executor.GalaxyExecutorApplication;
import com.mesalab.executor.core.config.StorgeConfig;
import com.mesalab.executor.core.utils.*;
import com.mesalab.executor.exception.BusinessException;
import com.mesalab.executor.jobhandler.LogStorageQuotaJob;
import com.mesalab.executor.pojo.JDBCParam;
import com.mesalab.executor.service.StorageQuotaService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {GalaxyExecutorApplication.class})
@Slf4j
public class StorageQuotaTest {

    @Autowired
    LogStorageQuotaJob logStorageQuotaJob;

    /**
     * 文件清库 慎
     */
    @Test
    public void delteAllFilesTest() {

//		ReturnT<String> deleteAllFiles = logStorageQuotaJob.deleteAllFiles("{\"maxdays\":365}");
//		Assert.isTrue(200==(deleteAllFiles.getCode()),"success");
    }


    @Test
    public void testS() {
        String filesHosServer = "Nur-sultan|192.168.40.223:9098,Aktau|192.168.1.2:9098,Aktubinsk|,Almaty|,Atyrau|,Karaganda|,Kokshetau|,Kostanay|,Kyzylorda|,Pavlodar|,Semey|,Shymkent|,Taldykurgan|,Taraz|,Uralsk|,Ust-Kamenogorsk|,Zhezkazgan|";

//		Splitter.on(",").splitToList(a).stream().map(x ->  x.substring(x.indexOf("|")));
        String b = "Nur-sultan|192.168.40.223:9098";
//		String collect = Splitter.on(",").splitToList(a).stream().map(x -> x.substring(x.indexOf("|") + 1)).filter(x-> StringUtil.isNotEmpty(x)).collect(Collectors.joining(","));
        List collect = Splitter.on(",").splitToList(filesHosServer).stream().map(x -> {
            List<String> dc = Splitter.on("|").splitToList(x);
            if (dc.size() == 2 && StringUtil.isNotBlank(dc.get(1))) {
                return ImmutableMap.of(dc.get(0), dc.get(1));
            }
            return null;
        }).filter(Objects::nonNull).collect(Collectors.toList());
        log.info(JsonMapper.toJsonString(collect));
    }

    @Test
    public void test() {
        String date = DateUtils.getDateOfYesterday("yyyyMMdd");
        long yyyyMMdd = DateUtils.convertStringToTimestamp(date, "yyyyMMdd");
        log.info(String.valueOf(yyyyMMdd));
    }

    @Test
    public void testStorage() {
        String msgc = "{\"log_type\":\"Files\",\"aggregate_size\":0,\"data_center\":\"Nur-sultan\",\"time\":1630569900,\"last_storage\":1622299518,\"used_size\":1217383982783,\"max_size\":25169958203392}";
        new KafkaUtils().sendMessage("SYS-STORAGE-LOG", msgc);
    }


    @Resource
    StorageQuotaService storageQuotaService;

    static Map<String, String> sourceParams = new HashMap<>();

    static {

        sourceParams.put("ip", "192.168.44.12");
        sourceParams.put("type", "mariadb");
        sourceParams.put("database", "tsg_olap");
        sourceParams.put("table", "sys_storage_event");
        sourceParams.put("username", "root");
        sourceParams.put("pin", "galaxy2019");

    }

    static JDBCParam jdbcParam = new JDBCParam(sourceParams);

    @Test
    public void testStorage2() {

        storageQuotaService.getAndSaveStorageQuotaInfo(new String[]{Constant.TRAFFIC_LOGS}, jdbcParam, true);
        storageQuotaService.getAndSaveStorageQuotaInfo(new String[]{Constant.METRICS}, jdbcParam, true);
        storageQuotaService.getAndSaveStorageQuotaInfo(new String[]{Constant.FILES}, jdbcParam, true);

    }

    @Resource
    StorgeConfig storgeConfig;

    @Test
    public void testQueryCk() {
        Map<String, Object> ckParamMap = storgeConfig.getCkSource();

        ckParamMap.put("query", "SELECT DISTINCT(`partition`) as date, SUM(`bytes_on_disk`) as bytes  from system.parts_cluster  WHERE database = 'tsg_galaxy_v3' and active = 1 group by date FORMAT JSON;");

        String result = HttpClientUtils.httpGet(HttpClientUtils.getUrlWithParams(UrlUtil.getUrl("192.168.44.67:8124"), ckParamMap));
        Map resultMap = JSONUtil.toBean(result, Map.class);
        List<Map> dataList = (List) resultMap.get("data");
        List<DailyStorage> dailyStorageList = Lists.newArrayList();
        for (Map map : dataList) {
            DailyStorage dailyStorage = new DailyStorage();
            dailyStorage.setDate(DateUtils.convertStringToTimestamp(String.valueOf(map.get("date")), "yyyyMMdd"));
            dailyStorage.setBytes(TypeUtils.castToLong(map.get("bytes")));
            dailyStorageList.add(dailyStorage);
        }

        Collections.sort(dailyStorageList, Comparator.comparingLong(DailyStorage::getDate).reversed());
        for (DailyStorage dailyStorage : dailyStorageList) {
            System.err.println(DateUtils.convertTimestampToString(dailyStorage.getDate(), "yyyyMMdd"));
            long maxDays = DateUtil.betweenDay(DateUtil.date(dailyStorage.getDate() * 1000), DateUtil.date(), true);
            System.err.println(maxDays);

        }

    }

    // datasource NOT LIKE '%hot%' and datasource not in ('traffic_general_stat') and
    @Test
    public void testQueryDruid() {
        String getDruidDailyUsedSizeSql = "{\"query\":\"SELECT \\\"start\\\" as \\\"date\\\", sum(\\\"size\\\") as bytes from sys.segments where is_published = 1 and \\\"start\\\" < '3000' " + storgeConfig.getNotInSql("datasource", storgeConfig.getAnalyticExclusion()) + " group by \\\"start\\\" ORDER BY \\\"start\\\" \",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";
        String result = HttpClientUtils.httpPost(UrlUtil.getUrl(storgeConfig.getAnalyticServer(), Constant.DRUID_URL), getDruidDailyUsedSizeSql);
        List<Map> dataList = JSONUtil.toList(result, Map.class);
        List<DailyStorage> dailyStorageList = Lists.newArrayList();
        for (Map map : dataList) {
            DailyStorage dailyStorage = new DailyStorage();
            dailyStorage.setDate(DateUtils.convertStringToTimestamp(String.valueOf(map.get("date")), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
            dailyStorage.setBytes(TypeUtils.castToLong(map.get("bytes")));
            dailyStorageList.add(dailyStorage);
        }
        System.err.println(dailyStorageList);
    }

    @Test
    public void testQueryMysql() {
        String mysqlSql = "SELECT UNIX_TIMESTAMP(FROM_UNIXTIME(generated_time,'%Y%m%d')) AS date, sum(bytes) AS used_size FROM sys_storage_event WHERE log_type='Files' AND data_center='xxg' GROUP BY date, log_type ORDER BY date ASC";

//        try (Connection conn = DBUtils.getDBConn(jdbcParam)) {
//            List<DailyStorage> dailyStorageList = SqlExecutor.query(conn, mysqlSql, new BeanListHandler<>(DailyStorage.class));
//            log.info(dailyStorageList.toString());
//        } catch (Exception e) {
//            log.error(e.getMessage());
//        }
        System.err.println(DateUtil.parseDate(DateUtil.today()).getTime());
    }

//    public final String clickhouseDailyUsedSizeSql = StrUtil.format("SELECT DISTINCT(`partition`) as date, SUM(`bytes_on_disk`) as bytes  from {}  WHERE database = '{}' {} and date <=  formatDateTime(now(),'%Y%m%d') and active = 1 group by date;",
//            storgeConfig.getSystemPartsCluster(), storgeConfig.getTrafficDatasource(), storgeConfig.getNotInSql("table", storgeConfig.getTafficExclusion()));
//    public final String druidDailyUsedSizeSql = "{\"query\":\"SELECT \\\"start\\\" as \\\"date\\\", sum(\\\"size\\\") as bytes from sys.segments where is_published = 1 and \\\"start\\\" < '3000' " + storgeConfig.getNotInSql("datasource", storgeConfig.getAnalyticExclusion()) + " group by \\\"start\\\" ORDER BY \\\"start\\\" \",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";

    private List<DailyStorage> getClickhouseDaily(String dataCenterHost) {
        Map<String, Object> ckParamMap = storgeConfig.getCkSource();
        ckParamMap.put("query", "clickhouseDailyUsedSizeSql");
        String result = HttpClientUtils.httpGet(HttpClientUtils.getUrlWithParams(UrlUtil.getUrl(dataCenterHost), ckParamMap));
        if ("-1".equals(result)) {
            throw new BusinessException("Get clickhouse http fail -1");
        }
        Map resultMap = JSONUtil.toBean(result, Map.class);
        List<Map> dataList = (List) resultMap.get("data");
        List<DailyStorage> dailyStorageList = com.google.common.collect.Lists.newArrayList();
        for (Map map : dataList) {
            DailyStorage dailyStorage = new DailyStorage();
            dailyStorage.setDate(DateUtils.convertStringToTimestamp(String.valueOf(map.get("date")), "yyyyMMdd"));
            dailyStorage.setBytes(TypeUtils.castToLong(map.get("bytes")));
            dailyStorageList.add(dailyStorage);
        }

        return dailyStorageList;
    }

    private List<DailyStorage> getDruidDaily(String dataCenterHost) {
        String result = HttpClientUtils.httpPost(UrlUtil.getUrl(dataCenterHost, Constant.DRUID_URL), "druidDailyUsedSizeSql");
        if ("-1".equals(result)) {
            throw new BusinessException("Get druid http fail -1");
        }
        List<Map> dataList = JSONUtil.toList(result, Map.class);
        List<DailyStorage> dailyStorageList = com.google.common.collect.Lists.newArrayList();
        for (Map map : dataList) {
            DailyStorage dailyStorage = new DailyStorage();
            dailyStorage.setDate(DateUtils.convertStringToTimestamp(String.valueOf(map.get("date")), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
            dailyStorage.setBytes(TypeUtils.castToLong(map.get("bytes")));
            dailyStorageList.add(dailyStorage);
        }
        return dailyStorageList;
    }

    @Data
    class DailyStorage {
        private long bytes;
        private long date;
    }
}
