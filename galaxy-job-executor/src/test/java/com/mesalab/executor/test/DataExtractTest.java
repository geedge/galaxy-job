package com.mesalab.executor.test;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.io.file.FileReader;
import cn.hutool.core.io.file.FileWriter;
import cn.hutool.core.map.CaseInsensitiveMap;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpUtil;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import cn.hutool.poi.excel.ExcelReader;
import cn.hutool.poi.excel.ExcelUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.mesalab.executor.core.utils.JobUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zdjizhi.utils.DateUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
            int count =0;
            FileReader fr = new FileReader(f);
            String s = fr.readString();
            Map<String,List> objMap = JSONUtil.toBean(s, Map.class);
            List<Map<String,String>> data = objMap.get("data");
            HashMap<Object, Object> m1 = new HashMap<>();
            List<Map<String,String>> list = new ArrayList<>();
            for (Map<String, String> datum : data) {
                if(datum.get("ioc_type").equals("url") ||datum.get("ioc_type").equals("domain") || datum.get("ioc_type").equals("ip:port")){
                    list.add(datum);
                    count++;
                }
            }
            System.err.println("获取数据： "+list.size());
            System.err.println("获取数据2： "+count);
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
        System.err.println(ipport1.substring(0,ipport1.lastIndexOf(":")));
        System.err.println(ObjectUtil.isNull(str)? "":str);
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
    public void dbTest(){
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
        }catch(Exception ex) {
            ex.printStackTrace();
            System.out.println("failure:"+ex.getMessage());
        }

    }

    private Log logger = Log.get();

    @Test
    public void fileTest(){
        HashMap<Object, Object> map = new HashMap<>();
        HashMap<Object, Object> map1 = new HashMap<>();
        HashMap<Object, Object> map2 = new HashMap<>();
        HashMap<Object, Object> map3 = new HashMap<>();
        map.put("A","b");
        map.put("B","b");
        map.put("C","b");
        map.put("D","b");
        map1.put("D","b");
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
        System.err.println("################# "+filePath);
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
    public void spider(){
        HashMap<String, Object> map = new HashMap<>();
        HttpUtil.downloadFile("https://threatfox-api.abuse.ch/api/v1?query=get_iocs&days=7","D:\\test\\test\\2\\");
    }
    @Test
    public void http(){
        String type = null;
        switch(type){
            case "mariadb":
                System.err.println(1);
                break;
            case "mysql":
                System.err.println(1);
                break;
            case "file":
                System.err.println(1);
                break;
            default:
                break;
        }
    }


}