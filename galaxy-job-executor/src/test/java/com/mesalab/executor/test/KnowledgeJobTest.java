package com.mesalab.executor.test;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.file.FileWriter;
import cn.hutool.core.io.resource.ResourceUtil;
import cn.hutool.core.text.csv.*;
import cn.hutool.core.util.CharsetUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.util.TypeUtils;
import com.alibaba.nacos.api.config.annotation.NacosValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.InetAddresses;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.core.utils.JobUtil;
import com.mesalab.executor.core.utils.ZookeeperUtils;
import com.mesalab.executor.service.knowledgeBase.KnowledgeBaseService;
import com.zdjizhi.utils.IPUtil;
import com.zdjizhi.utils.IpLookupV2;
import org.apache.tomcat.util.net.IPv6Utils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.BufferedReader;
import java.io.File;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.*;

import static com.mesalab.executor.core.utils.Constant.ZK_KNOWLEDGE_PATH;
import static org.junit.Assert.assertNotNull;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class KnowledgeJobTest {

    @Test
    public void getIPMMDBFile() {
        IpLookupV2 ipLookup = new IpLookupV2.Builder(false)
//                .loadDataFileV4("C:\\code\\galaxy-qgw-service\\dat\\ip_v4_built_in.mmdb")
//                .loadDataFileV6("C:\\code\\galaxy-qgw-service\\dat\\ip_v6_built_in.mmdb")
//                .loadDataFilePrivateV4("C:\\code\\galaxy-qgw-service\\dat\\ip_v6_built_in.mmdb")
//                .loadDataFile("C:\\test\\1.mmdb")
                .loadDataFileV4("C:\\test\\ip4.mmdb")
                .loadAsnDataFile("C:\\test\\knowledge\\ansv6.mmdb")
//                .loadDataFilePrivateV6("")
                .build();

//        String latLngLookup = ipLookup.latLngLookup("0.0.0.0");
//        System.err.println(latLngLookup);
//        System.err.println(ipLookup.latLngLookup("192.168.1.1"));
        System.err.println(ipLookup.asnLookupInfo("2c0f:fc89:ec::"));
//        System.err.println(ipLookup.countryLookup("0.0.0.0"));
//        System.err.println(ipLookup.cityLookup("192.168.1.1"));
//        System.err.println(ipLookup.asnLookupDetail("1.0.0.0"));
//        System.err.println(ipLookup.asnLookupDetail("8.6.146.255"));

        Assert.assertNotEquals("ip is not found", ObjectUtil.isEmpty("latLngLookup"));


    }

    @Test
    public void pushKnowledge() {
        String knowledgeServer = "http://192.168.45.102:9999/knowledge_base/v1";
        String filePath = "C:\\code\\galaxy-qgw-service\\dat\\ip_v4_built_in.mmdb";
//        HttpResponse response = HttpRequest.post(knowledgeServer)
//                .form("file", new File(filePath))
//                .form("name", "ip_v6_user_defined")
//                .form("format", "mmdb")
//                .form("type", "ip_location")
//                .timeout(20000)//超时，毫秒
//                .execute();
//        int status = response.getStatus();
//        String body = response.body();
        File file = new File(filePath);
        Map<String, Object> paramsMap = ImmutableMap.of("name", "ip_v6_user_defined", "format", "mmdb", "type", "ip_location");
        String resultStr = HttpClientUtils.httpPostFile(knowledgeServer, file, paramsMap);

        if (!"-1".equals(resultStr)) {
            Map result = JSONUtil.toBean(resultStr, Map.class);
            Assert.assertTrue("success", result.get("status") != null && "200".equals(String.valueOf(result.get("status"))));
        }

    }

    @Test
    public void asnDataFormat() {
        CsvReader reader = CsvUtil.getReader();
        String ansCsvFile = "D:\\test\\knowledge\\ans_v4.csv";
        CsvData data = reader.read(FileUtil.file(ansCsvFile));
        List<CsvRow> rows = data.getRows();
        //遍历行
        List<String> newList = new ArrayList();
        for (CsvRow csvRow : rows) {
            String raw = csvRow.getRawList().get(0);
            if (!newList.contains(raw)) {
                newList.add(raw);
            }
        }
        List<Map> result = new ArrayList<>();
        FileWriter fileWriter = new FileWriter("D:\\test\\knowledge\\update.sql");
        for (String s : newList) {
//            Map<String, Object> map = new LinkedHashMap<>();
//            map.put("fqdn", s);
//            result.add(map);
            fileWriter.write("update domain_category_reputation set submit_user='tsg' where fqdn='" + s + "';\n", true);
        }

    }
    @Test
    public void ipv6Format() {
        CsvReader reader = CsvUtil.getReader();
        String ansCsvFile = "C:\\test\\knowledge\\tsgv6.csv";
        String newCsv = "C:\\test\\knowledge\\newv6.csv";
        CsvData data = reader.read(FileUtil.file(ansCsvFile));
        List<CsvRow> rows = data.getRows();
        //遍历行

        CsvWriter writer = CsvUtil.getWriter(newCsv, CharsetUtil.CHARSET_UTF_8);
        int count =0;
        for (CsvRow csvRow : rows) {
            List<String> rawList = csvRow.getRawList();
            if(count>0){
                String startIp = csvRow.get(1);
                String endIp = csvRow.get(2);
                rawList.add(IpConvert.stringToBigInt(startIp).toString());
                rawList.add(IpConvert.stringToBigInt(endIp).toString());

            } else {
                rawList.add("minip");
                rawList.add("maxip");
            }
            count++;
            writer.write(rawList.toArray(new String[rawList.size()]));
        }
        writer.close();


    }

    @Test
    public void asnTest(){
        try {
            boolean asn = KnowledgeBaseService.checkIpMMDBSample("C:\\test\\knowledge\\", "asn6.mmdb", "asn");
            Assert.assertTrue(asn);
        } catch (Exception e) {
            JobUtil.errorLog(e);
        }
    }

    @NacosValue(value = "${zookeeper.server}", autoRefreshed = true)
    private String zookeeperServer;
    @Test
    public void zkTest() throws Exception{
            try {
                ZookeeperUtils zk = new ZookeeperUtils();
                List<String> children = zk.getChildren(ZK_KNOWLEDGE_PATH, zookeeperServer);
//                HashMap<String, Object> map = new HashMap<>();
//                map.put("knowledge_id1", "100002");
//                map.put("knowledge_id2", 3);
//                children.remove(map.get("knowledge_id1"));
//                children.remove("sadfasd");
//                assertNotNull(children);
                System.err.println(children);
            } catch (Exception e) {
                JobUtil.errorLog("knowledgeBase query zookeeper error :{}", e);
            }
    }
    @Test
    public void zk1Test() throws Exception{
        try {
            ZookeeperUtils zk = new ZookeeperUtils();
            boolean b = zk.deleteNode("/test/1", zookeeperServer);
//                HashMap<String, Object> map = new HashMap<>();
//                map.put("knowledge_id1", "100002");
//                map.put("knowledge_id2", 3);
//                children.remove(map.get("knowledge_id1"));
//                children.remove("sadfasd");
//                assertNotNull(children);
            System.err.println(b);
        } catch (Exception e) {
            JobUtil.errorLog("knowledgeBase query zookeeper error :{}", e);
        }
    }


}