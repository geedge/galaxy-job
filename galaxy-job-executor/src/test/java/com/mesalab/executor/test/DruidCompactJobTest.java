package com.mesalab.executor.test;

import com.alibaba.fastjson.JSONObject;
import com.google.common.io.CharStreams;
import com.mesalab.executor.GalaxyExecutorApplication;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.jobhandler.DruidCompactJob;
import com.mesalab.executor.pojo.DruidNativeCompactParam;
import com.mesalab.executor.pojo.DruidToHttpParam;
import com.xxl.job.core.log.XxlJobLogger;
import com.zdjizhi.utils.JsonMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.print.DocFlavor;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import static jdk.nashorn.internal.runtime.regexp.joni.Config.log;

/**
 * @ Author pengfeixu
 * @ Date 2021/7/12
 * @ Description
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {GalaxyExecutorApplication.class})
@Slf4j
public class DruidCompactJobTest {
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
                .append("/druid/indexer/v1/task");
//        druidNativeCompactParam.setDataSource("top_website_domain_log");
//        druidNativeCompactParam.setNumOfDay("1");
//        druidNativeCompactParam.setSegmentGranularity("DAY");
//        String dataSource = druidCompactJob.getDataSource(druidNativeCompactParam);
//        String timeInterval = druidCompactJob.getTimeInterval(druidNativeCompactParam);
//        String segmentGranularity = druidCompactJob.getSegmentGranularity(druidNativeCompactParam);
        String json="{\n" +
                "    \"type\":\"compact\",\n" +
                "    \"dataSource\":\"top_website_domain_log\",\n" +
                "    \"ioConfig\":{\n" +
                "        \"type\":\"compact\",\n" +
                "        \"inputSpec\":{\n" +
                "            \"type\":\"interval\",\n" +
                "            \"interval\":\"2021-07-09/2021-07-10\"\n" +
                "        }\n" +
                "    },\n" +
                "    \"segmentGranularity\":\"DAY\",\n" +
                "    \"tuningConfig\":{\n" +
                "        \"type\":\"index_parallel\",\n" +
                "        \"maxRowsPerSegment\":5000000,\n" +
                "        \"maxRowsInMemory\":25000\n" +
                "    }\n" +
                "}";
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
        HttpClientUtils.httpPost(url.toString(),json);
    }
}
