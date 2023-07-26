package com.mesalab.executor.test;

import com.fasterxml.jackson.databind.JavaType;
import com.mesalab.executor.GalaxyExecutorApplication;
import com.mesalab.executor.core.config.StorgeConfig;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.core.utils.SpringContextUtil;
import com.mesalab.executor.core.utils.UrlUtil;
import com.mesalab.executor.pojo.DruidToHttpParam;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;

/**
 * @ Author pengfeixu
 * @ Date 2021/7/12
 * @ Description
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Slf4j
public class DruidCompactJobTest {
    //    private static StorgeConfig compactConfig =  (StorgeConfig) SpringContextUtil.getBean("storgeConfig");
    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
    private static JavaType hashmapJsonType = jsonMapperInstance.createCollectionType(HashMap.class, String.class, Object.class);
    /**
     * 压缩测试
     *
     * 原提交请求方式
     * curl -X 'POST' -H 'Content-Type:application/json -d @*.json http://ip:8089/druid/indexer/v1/task
     */
    @Test
    public void  compactTest() throws ParseException, IOException {
        DruidToHttpParam druidToHttpParam = new DruidToHttpParam();
        druidToHttpParam.setUrl("192.168.44.12");
        druidToHttpParam.setPort("8089");
        StringBuilder url = new StringBuilder("http://");
        url.append(druidToHttpParam.getUrl())
                .append(":")
                .append(druidToHttpParam.getPort())
                .append("/druid/v2/sql/");
//        druidNativeCompactParam.setDataSource("top_website_domain_log");
//        druidNativeCompactParam.setNumOfDay("1");
//        druidNativeCompactParam.setSegmentGranularity("DAY");
//        String dataSource = druidCompactJob.getDataSource(druidNativeCompactParam);
//        String timeInterval = druidCompactJob.getTimeInterval(druidNativeCompactParam);
//        String segmentGranularity = druidCompactJob.getSegmentGranularity(druidNativeCompactParam);
//        String json="{\n" +
//                "    \"type\":\"compact\",\n" +
//                "    \"dataSource\":\"top_website_domain_log\",\n" +
//                "    \"ioConfig\":{\n" +
//                "        \"type\":\"compact\",\n" +
//                "        \"inputSpec\":{\n" +
//                "            \"type\":\"interval\",\n" +
//                "            \"interval\":\"2021-07-09/2021-07-10\"\n" +
//                "        }\n" +
//                "    },\n" +
//                "    \"segmentGranularity\":\"DAY\",\n" +
//                "    \"tuningConfig\":{\n" +
//                "        \"type\":\"index_parallel\",\n" +
//                "        \"maxRowsPerSegment\":5000000,\n" +
//                "        \"maxRowsInMemory\":25000\n" +
//                "    }\n" +
//                "}";
//        Map<String,Object> jsonToRequestBody=new HashMap<String,Object>(){
//            {
//                put("type","compact");
//                put("datasource",druidCompactJob.getDataSource(druidNativeCompactParam));
//                put("ioConfig",new HashMap(){
//                    {
//                        put("type","compact");
//                        put("inputSpec",new HashMap(){
//                            {
//                                put("type","interval");
//                                put("interval",druidCompactJob.getTimeInterval(druidNativeCompactParam));
//                            }
//                        });
//                    }
//                });
//                put("segmentGranularity",druidCompactJob.getSegmentGranularity(druidNativeCompactParam));
//                put("tuningConfig",new HashMap(){
//                    {
//                        put("type","index_parallel");
//                        put("maxRowsPerSegment",5000000);
//                        put("maxRowsInMemory",25000);
//                    }
//                });
//            }
//        };
//        String url1 = UrlUtil.getUrl(compactConfig.getAnalyticServer(), url.toString());
        String json="{ \"query\" : \"select count(1) from traffic_general_stat where __time >= TIMESTAMP '2023-07-07 00:00:00'  and __time < TIMESTAMP '2023-07-08 00:00:00'\", \"resultFormat\" : \"objectLines\" }";
//        String json="select count(1) from application_protocol_stat";

//        String s = HttpClientUtils.httpPost(url1, json);
//        String s = HttpClientUtils.httpPost(url.toString(), JsonMapper.toJsonString(json));
        String s = HttpClientUtils.httpPost(url.toString(), json);
        if (StringUtil.isNotBlank(s)){
            HashMap<String, Integer> value = jsonMapperInstance.fromJson(s, hashmapJsonType);
            int countValue=Integer.parseInt(value.get("EXPR$0").toString());
            if (countValue >0){
                System.out.println(countValue);
            }
        }



//        String s1 = JsonMapper.toJsonString(s);
//        System.out.println(s1);
//        String[] split = s.split(":");
//        System.out.println(split[1]);
//        System.out.println(s);
    }
}
