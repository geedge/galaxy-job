package com.mesalab.executor.test;

import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.io.file.FileReader;
import cn.hutool.core.io.file.FileWriter;
import cn.hutool.core.lang.Console;
import cn.hutool.core.map.CaseInsensitiveMap;
import cn.hutool.core.net.url.UrlBuilder;
import cn.hutool.core.text.csv.CsvData;
import cn.hutool.core.text.csv.CsvReader;
import cn.hutool.core.text.csv.CsvRow;
import cn.hutool.core.text.csv.CsvUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpUtil;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import cn.hutool.poi.excel.ExcelReader;
import cn.hutool.poi.excel.ExcelUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.mesalab.executor.core.utils.DBUtils;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.core.utils.JobUtil;
import com.mesalab.executor.core.utils.UrlUtil;
import com.mesalab.executor.core.utils.async.HttpKit;
import com.mesalab.executor.enums.DataTypeEnum;
import com.mesalab.executor.exception.BusinessException;
import com.mesalab.executor.pojo.JDBCParam;
import com.mesalab.executor.service.DataSinkService;
import com.mesalab.executor.service.DataSourceService;
import com.mesalab.executor.service.DataTransformService;
import com.xxl.job.core.log.XxlJobLogger;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zdjizhi.utils.DateUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class DataExtractTest {

    @Test
    public void excelTest() {
//        File f1 = FileUtil.file("D:\\test\\test\\");
//        String type = FileTypeUtil.getType(f1);
        List<File> files = FileUtil.loopFiles(Paths.get("D:\\test\\test\\"), 1, FileFilterUtils.suffixFileFilter("xlsx"));

        List<Map> collect = files.stream().map(f -> {
            System.err.println(f.getName());
            ExcelReader reader = ExcelUtil.getReader(f);
            List<String> sheetNames = reader.getSheetNames();
            Map<String, List<Map>> sheetMap = new HashMap();
            for (String sheetName : sheetNames) {
                ExcelReader rd = ExcelUtil.getReader(f, sheetName);
                List<Map> maps = rd.readAll(Map.class);
                sheetMap.put(sheetName, maps);
                rd.close();
                IoUtil.close(rd);
            }
            IoUtil.close(reader);
            return sheetMap;
        }).collect(Collectors.toList());
        System.err.println(JSONUtil.parse(collect));
    }

    @Test
    public void jsonTest() {
        String path = "D:\\test\\test\\full2.json";
        List<File> files = FileUtil.loopFiles(Paths.get(path), 1, FileFilterUtils.suffixFileFilter("json"));
        List<Map> jsonList = files.stream().map(f -> {
            int count = 0;
            FileReader fr = new FileReader(f);
            String s = fr.readString();
            Map<String, List> objMap = JSONUtil.toBean(s, Map.class);
            List<Map<String, String>> data = objMap.get("data");
            HashMap<Object, Object> m1 = new HashMap<>();
            List<Map<String, String>> list = new ArrayList<>();
            for (Map<String, String> datum : data) {
                if (datum.get("ioc_type").equals("url") || datum.get("ioc_type").equals("domain") || datum.get("ioc_type").equals("ip:port")) {
                    list.add(datum);
                    count++;
                }
            }
            System.err.println("获取数据： " + list.size());
            System.err.println("获取数据2： " + count);
            return objMap;
        }).collect(Collectors.toList());
        System.err.println(jsonList.toString());
    }

    @Test
    public void webketckTest() {
        String[] fileTypes = {"json", "xlsx"};
        String path = "D:\\test\\test\\";
        for (String fileType : fileTypes) {
            Map<String, List> typeResult = new HashMap();
            if ("json".equals(fileType)) {
                List<File> files = FileUtil.loopFiles(Paths.get(path), 1, FileFilterUtils.suffixFileFilter(fileType));
                List<Map> jsonList = files.stream().map(f -> {
                    FileReader fr = new FileReader(f);
                    Map objMap = JSONUtil.toBean(fr.readString(), Map.class);
                    return objMap;
                }).collect(Collectors.toList());
                typeResult.put(fileType, jsonList);
            } else if ("xlsx".equals(fileType)) {
                List<File> files = FileUtil.loopFiles(Paths.get(path), 1, FileFilterUtils.suffixFileFilter(fileType));
                List<Map<String, List<Map>>> excelList = files.stream().map(f -> {
                    ExcelReader reader = ExcelUtil.getReader(f);
                    List<String> sheetNames = reader.getSheetNames();
                    IoUtil.close(reader);
                    Map<String, List<Map>> sheetMap = new HashMap();
                    for (String sheetName : sheetNames) {
                        ExcelReader rd = ExcelUtil.getReader(f, sheetName);
                        sheetMap.put(sheetName, rd.readAll(Map.class));
                        IoUtil.close(rd);
                    }
                    return sheetMap;
                }).collect(Collectors.toList());
                typeResult.put(fileType, excelList);
            }
            System.err.println(typeResult);
        }
    }

    @Test
    public void mapTest() {
        CaseInsensitiveMap map = new CaseInsensitiveMap();
        map.put("ABc", "asdf");
        System.err.print(map.containsKey("abc"));
        String ipport1 = "ip:port";
        String ipport2 = "ip:ip:port";
        Object str = null;
        System.err.println(ipport1.substring(0, ipport1.lastIndexOf(":")));
        System.err.println(ObjectUtil.isNull(str) ? "" : str);
    }

    @Test
    public void convertTest() {
        String path = "D:\\test\\test\\full.json";
        String path2 = "D:\\test\\test\\full2.json";
        File file = FileUtil.file(path);
        Map<Object, Object> newMap = new HashMap<>();
        ArrayList<Object> list = new ArrayList<>();
        FileReader fr = new FileReader(file);
//        String s = fr.readString();
        Map<String, List> objMap = JSONUtil.toBean(fr.readString(), Map.class);
        for (Map.Entry<String, List> map : objMap.entrySet()) {
            list.addAll(map.getValue());
        }
        newMap.put("data", list);
        FileWriter writer = new FileWriter(path2);
        writer.write(JSONUtil.toJsonPrettyStr(newMap));

//        System.err.println(jsonList.toString());
    }

    private static HikariDataSource dataSource;

    @Test
    public void dbTest() {
        HikariConfig config = new HikariConfig();
//        config.setDataSourceClassName("com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
//        config.addDataSourceProperty("url", "jdbc:mysql://192.168.44.12:3306/web_sketch?serverTimezone=UTC");
//        config.addDataSourceProperty("user","root");
//        config.addDataSourceProperty("password", "galaxy2019");


//        config.setDataSourceClassName("com.mysql.cj.jdbc.MysqlConnectionPoolDataSource");
        config.setJdbcUrl("jdbc:mysql://192.168.44.12:3306/web_sketch?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false");
        config.setUsername("root");
        config.setPassword("galaxy2019");

//
        config.setConnectionTestQuery("select 1");
        config.setMinimumIdle(10);
        config.setMaximumPoolSize(20);
        config.setConnectionTimeout(1000);
        config.setIdleTimeout(60000);
        config.setMaxLifetime(180000);
        dataSource = new HikariDataSource(config);

//        dataSource.setDataSourceClassName("com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
//        dataSource.setJdbcUrl("jdbc:mysql://192.168.44.12:3306/web_sketch?serverTimezone=UTC");
//        dataSource.setUsername("root");
//        dataSource.setPassword("galaxy2019");

        try {
            Connection connection = dataSource.getConnection();
            PreparedStatement pst = connection.prepareStatement("select count(*) from ioc_malware");
            boolean execute = pst.execute();
            pst.close();
            System.out.println("success");
            connection.close();
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("failure:" + ex.getMessage());
        }

    }

    private Log logger = Log.get();

    @Test
    public void fileTest() {
        HashMap<Object, Object> map = new HashMap<>();
        HashMap<Object, Object> map1 = new HashMap<>();
        HashMap<Object, Object> map2 = new HashMap<>();
        HashMap<Object, Object> map3 = new HashMap<>();
        map.put("A", "b");
        map.put("B", "b");
        map.put("C", "b");
        map.put("D", "b");
        map1.put("D", "b");
        List<Map> transformResult = new ArrayList();
        transformResult.add(map);
        transformResult.add(map1);
        transformResult.add(map2);
        transformResult.add(map3);

        String fileType = "json";
        String path = "D:\\test\\test\\1\\";
        String prefix = "test";
        String date = DateUtils.getCurrentDate("yyyyMMdd");
        String filePath = StrUtil.concat(false, path, prefix, "-", date);
        System.err.println("################# " + filePath);
        if ("json".equals(fileType)) {
//            FileWriter fileWriter = new FileWriter(filePath.concat(".json"));
//            File file = fileWriter.write(JSONUtil.toJsonStr(transformResult));
            File file = new File(filePath.concat(".json"));
            BufferedWriter bw = null;
            try {
                bw = new BufferedWriter(new java.io.FileWriter(file));
//                String s = JSON.toJSONString(transformResult);
//                String s = JSONUtil.toJsonPrettyStr(transformResult);
                String s1 = JSON.toJSONString(transformResult, SerializerFeature.PrettyFormat);
                bw.write(s1);
//            }
                bw.flush();
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }


            logger.info("download file to path {}", filePath);
        } else if ("csv".equals(fileType)) {
            File file = JobUtil.createCsvFile(transformResult, StrUtil.concat(false, filePath, ".csv"));
            logger.info("download file to path {}", file.getAbsolutePath());
        }
    }

    @Test
    public void spider() {
        HashMap<String, Object> map = new HashMap<>();
        HttpUtil.downloadFile("https://threatfox-api.abuse.ch/api/v1?query=get_iocs&days=7", "D:\\test\\test\\2\\");
    }

    @Test
    public void http() {
        String d = "g13ddlsoogle.com";
        try {
            String url = UrlBuilder.create().setHost(d).build();
            System.err.println(url);
            String s = HttpClientUtils.httpGet(url);
            System.err.println(s);
        } catch (BusinessException e) {
            System.err.println(e.getCause());
            System.err.println(e.getCode());
            System.err.println(e.getStackTrace());
            e.printStackTrace();
        }


    }

    @Autowired
    DataSourceService dataSourceService;

    public List<Map> getResult() {
        Map<String, String> sourceParams = new HashMap<>();
        sourceParams.put("ip", "192.168.44.12");
        sourceParams.put("type", "mariadb");
        sourceParams.put("database", "web_sketch");
        sourceParams.put("table", "domain_category_reputation2");
        sourceParams.put("username", "root");
        sourceParams.put("pin", "galaxy2019");
        sourceParams.put("sql", "SELECT id,fqdn,is_accessible FROM domain_category_reputation2 limit 1000");
        List<Map> mariadbResult = new ArrayList<>();
        try {
            Map<String, Object> sourceResult = dataSourceService.adapt(sourceParams);
            Map<String, List> source = JSONUtil.toBean(JSON.toJSONString(sourceResult, SerializerFeature.WriteNullStringAsEmpty), Map.class);
            mariadbResult = source.get(DataTypeEnum.MARIADB.getName());

        } catch (Exception e) {
            e.printStackTrace();
        }
        return mariadbResult;
    }


    @Autowired
    DataSinkService dataSinkService;

    @Test
    public void domainThreadPool() {

        long start = System.currentTimeMillis();
//        Vector<Map> maps = new Vector<>();
//        Queue<Map> maps = new ConcurrentLinkedQueue<Map>();
        List maps = new CopyOnWriteArrayList();
//        List maps = new ArrayList();

        List<Map> list = getResult();
        int threadNum = 500;
        int partitionSize = (list.size() / threadNum) > 0 ? list.size() / threadNum : 1;
        List<List<Map>> split = ListUtil.split(list, partitionSize);

        System.err.println("开始时间:" + new Date());
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(split.size());
        System.err.println("当前线程数为：" + split.size());
        // 用于计数线程是否执行完成
        CountDownLatch countDownLatch = new CountDownLatch(split.size());
        for (List<Map> mapList : split) {
            fixedThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    for (Map map : mapList) {
                        Map m = new ConcurrentHashMap();
                        String fqdn = UrlUtil.getUrl(map.get("fqdn"));
                        int code = HttpKit.getStatus(fqdn);
                        m.put("code", code);
                        m.put("fqdn", map.get("fqdn"));
                        maps.add(m);
                    }
                    countDownLatch.countDown();
                }
            });
        }
        System.err.println("thread over !!!!!!!!!!!!");
        try {
            countDownLatch.await();
            fixedThreadPool.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
            fixedThreadPool.shutdownNow();
        }

        System.err.println("结束时间： " + new Date());
        System.err.println("花费时间： " + (System.currentTimeMillis() - start));
        System.err.println(maps.size());
    }

    @Test
    public void domain3() {
        System.err.println("开始时间:" + new Date());
        long start = System.currentTimeMillis();
        Vector<Map> list = new Vector<>();
        List<Map> maps = getResult();
        System.err.println("预处理数据：" + maps.size());
        //
        int splitSize = 5;
        int maxSize = maps.size() / splitSize + 1;
        Stream.iterate(0, n -> n + 1).limit(maxSize).parallel().forEach(a -> {
            List<Map> sendList = maps.stream().skip(a * splitSize).limit(splitSize).parallel().collect(Collectors.toList());
            sendList.forEach(u -> {
                Map<String, Object> newMap = new ConcurrentHashMap<>();
                u.get("fqdn");
                int code = HttpKit.getStatus(UrlUtil.getUrl(String.valueOf(u.get("fqdn"))));
//                int code = HttpAsyncUtil.get(UrlUtil.getUrl(String.valueOf(u.get("fqdn"))));
//                int code = SimpleHttpAsyncClientDemo.send(UrlUtil.getUrl(String.valueOf(u.get("fqdn"))));
//                int code = HttpUtils.get(UrlUtil.getUrl(String.valueOf(u.get("fqdn"))));
                newMap.put("fqdn", u.get("fqdn"));
                newMap.put("code", code);
                list.add(newMap);
            });
        });
//        System.err.println(list);
//        ForkJoinPool forkJoinPool = new ForkJoinPool(25);
//        forkJoinPool.submit(() -> {
//            maps.parallelStream().forEach(u -> {
//                Map<String, Object> newMap = new ConcurrentHashMap<>();
//                int code = HttpKit.getStatus(UrlUtil.getUrl(u.get("fqdn")));
//                newMap.put("fqdn", u.get("fqdn"));
//                newMap.put("code", code);
//                System.err.println(newMap);
//                list.add(newMap);
//            });
//        });
//        forkJoinPool.awaitQuiescence(1,TimeUnit.MILLISECONDS);
//        forkJoinPool.shutdown();
        System.err.println("结束时间： " + new Date());
        System.err.println("花费时间： " + (System.currentTimeMillis() - start));
        System.err.println("结果条数： " + list.size());
    }

    @Test
    public void test12() {
//        ArrayList<Object> objects = new ArrayList<>();
//        objects.add(1);
//        objects.add(2);
//        objects.add(3);
//        objects.add(4);
//
////        System.err.println(ListUtil.split(objects, 3));
////        System.err.println(Lists.partition(objects, 3));
        String fileTypeParams = "json";
        String[] fileTypes = StrUtil.split(fileTypeParams, ",");
        System.err.println(Arrays.toString(fileTypes));
    }

    @Test
    public void csvUpdate() {
        CsvReader reader = CsvUtil.getReader();
        //从文件中读取CSV数据
        CsvData data = reader.read(FileUtil.file("D:\\test\\web\\update0421.csv"));
        List<CsvRow> rows = data.getRows();
        //遍历行
        List<String> newList = new ArrayList();
        for (CsvRow csvRow : rows) {
            //getRawList返回一个List列表，列表的每一项为CSV中的一个单元格（既逗号分隔部分）
            String raw = csvRow.getRawList().get(0);
            if (!newList.contains(raw)) {
                newList.add(raw);
            }
        }
        List<Map> result = new ArrayList<>();
        FileWriter fileWriter = new FileWriter("D:\\test\\web\\update.sql");
        for (String s : newList) {
//            Map<String, Object> map = new LinkedHashMap<>();
//            map.put("fqdn", s);
//            result.add(map);
            fileWriter.write("update domain_category_reputation set submit_user='tsg' where fqdn='" + s + "';\n", true);
        }
//        dbUpdate(result);
    }

    @Test
    public void txtUpdate() {

        String operator = "FWDNS";
        FileReader fr = new FileReader("D:\\test\\test\\ip\\" + operator + ".txt");
        List<String> strings = fr.readLines();
        FileWriter fw = new FileWriter("D:\\test\\test\\ip\\" + operator + ".sql");
        for (String s : strings) {
            fw.write("INSERT IGNORE INTO dns_server_info " +
                    "(ip_addr, dns_server_role, create_time, update_time) " +
                    "VALUES('" + StrUtil.trim(s) + "', '" + operator + "', current_timestamp(), current_timestamp());\n", true);
        }
    }

    public void dbUpdate(List<Map> transformResult) {
        Map<String, String> sinkParams = new HashMap<>();
        sinkParams.put("ip", "api.geedge.net");
        sinkParams.put("type", "mariadb");
        sinkParams.put("database", "web_sketch_v2");
        sinkParams.put("table", "domain_category_reputation");
        sinkParams.put("username", "root");
        sinkParams.put("pin", "webskt2021");
        Connection conn = null;
        DBUtils dbUtils = null;
        PreparedStatement pst = null;
        try {
            String tableName = sinkParams.get("table");
            dbUtils = new DBUtils();
            conn = dbUtils.getDBConn(new JDBCParam(sinkParams));
            final String submitUser = "'tsg'";
            String sql = "update " + sinkParams.get("table") + " set submit_user=" + submitUser + " where fqdn=?";
            System.err.println(sql);
            long start = System.currentTimeMillis();
            conn.setAutoCommit(false);
            pst = conn.prepareStatement(sql);
            int count = 0;
            int index = 1;
            for (int i = 0; i < transformResult.size(); i++) {
                for (Object val : transformResult.get(i).values()) {
                    if (val instanceof Long) {
                        pst.setLong((index++), Long.valueOf(StrUtil.toString(val)));
                    } else if (val instanceof Integer) {
                        pst.setInt((index++), Integer.valueOf(StrUtil.toString(val)));
                    } else if (val instanceof Boolean) {
                        pst.setBoolean((index++), Boolean.valueOf(StrUtil.toString(val)));
                    } else {
                        pst.setString((index++), StrUtil.toString(val));
                    }
                }
                System.err.println(pst.toString());
                index = 1;
                pst.addBatch();
                //1w提交一次
                if (i % 100 == 0) {
                    int[] ints = pst.executeBatch();
                    count = count + ints.length;
                    conn.commit();
                    pst.clearBatch();
                }
            }
            int[] ints = pst.executeBatch();
            count = count + ints.length;
            conn.commit();
            Console.log("{} table {} , count {} ,  take {}", sinkParams.get("option"), tableName, count, (System.currentTimeMillis() - start) + "ms");
        } catch (SQLException e) {
            Console.error(e.getMessage());
        } finally {
            IoUtil.close(pst);
            IoUtil.close(conn);
        }
    }

    static Map<String, Map> paramsMap = new HashMap<>();

    static {
        Map<String, String> sourceParams = new HashMap<>();
        sourceParams.put("ip", "192.168.44.12");
        sourceParams.put("type", "mariadb");
        sourceParams.put("database", "web_sketch");
        sourceParams.put("table", "domain_category_reputation");
        sourceParams.put("username", "root");
        sourceParams.put("pin", "galaxy2019");
        sourceParams.put("sql", "SELECT id,fqdn,status_code FROM domain_category_reputation");
        sourceParams.put("sharding", "5");
        paramsMap.put("source", sourceParams);
    }

    @Autowired
    DataTransformService dataTransformService;

    @Test
    public void shardingTest() {
        Map<String, String> params = new HashMap<>();
        params.put("parallelism", "5");
        Map<String, String> sinkParams = new HashMap<>();
        sinkParams.put("type", "mariadb");
        sinkParams.put("ip", "192.168.44.12");
        sinkParams.put("database", "web_sketch");
        sinkParams.put("table", "domain_category_reputation");
        sinkParams.put("username", "root");
        sinkParams.put("pin", "galaxy2019");
        sinkParams.put("option", "update");
        try {
            long start = System.currentTimeMillis();
            Map<String, String> map = paramsMap.get("source");
            Map<String, String> countParams = new HashMap<>();
            countParams.putAll(map);
            countParams.put("sql", "select count(*) AS count from " + countParams.get("table"));
            List<Map> countMap = dataSourceService.mysqlSource(countParams);
            String parentSql = map.get("sql");
            int sharding = Integer.valueOf(map.get("sharding"));
            int count = Integer.valueOf(StrUtil.toString(countMap.get(0).get("count")));
            logger.info("query result data count {} , sharding {}", count, sharding);
            int shardingSize = count % sharding == 0 ? count / sharding : count / sharding + 1;
            for (int i = 0; i < sharding; i++) {
                logger.info("sharding {} source execute start", i + 1);
                XxlJobLogger.log("sharding {} source execute start", i + 1);
                String shardingSql = parentSql + " order by id desc limit " + i * shardingSize + "," + ((i + 1) * shardingSize > count ? count : (i + 1) * shardingSize);
                map.put("sql", shardingSql);
                List<Map> shardingResult = dataSourceService.mysqlSource(map);
                logger.info("sharding {} source execute end", i + 1);
                XxlJobLogger.log("sharding {} source execute end", i + 1);

                logger.info("sharding {} transform execute start", i + 1);
                XxlJobLogger.log("sharding {} transform execute start", i + 1);
                List<Map> transformResult = dataTransformService.isFqdnAccessible(shardingResult, params);
                logger.info("sharding {} transform execute end", i + 1);
                XxlJobLogger.log("sharding {} transform execute end", i + 1);

                logger.info("sharding {} sink execute start", i + 1);
                XxlJobLogger.log("sharding {} sink execute start", i + 1);
                dataSinkService.exec(transformResult, sinkParams);
                logger.info("sharding {} sink execute end", i + 1);
                XxlJobLogger.log("sharding {} sink execute end", i + 1);
            }
            logger.info("sharding task end, take time {}ms", System.currentTimeMillis() - start);
            XxlJobLogger.log("sharding task end, take time {}ms", System.currentTimeMillis() - start);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}