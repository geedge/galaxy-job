package com.mesalab.executor.test;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.ReUtil;
import cn.hutool.crypto.digest.DigestUtil;
import com.mesalab.executor.pojo.JDBCParam;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mesalab.executor.core.utils.HttpClientUtils.getHttpClient;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class SourceJobTest {


    @Test
    public void testTable() {

        Map<String, String> sourceParams = new HashMap<>();
        sourceParams.put("ip", "192.168.44.12");
        sourceParams.put("type", "mariadb");
        sourceParams.put("database", "web_sketch");
        sourceParams.put("username", "root");
        sourceParams.put("pin", "galaxy2019");
        sourceParams.put("sql", "SELECT id,fqdn,is_accessible FROM web_sketch.domain_category_reputation2 limit 1000");

        JDBCParam mysqlSourceParam = new JDBCParam(sourceParams);
        System.err.println(mysqlSourceParam.getTable());


    }


    @Test
    public void testTxt() throws IOException, URISyntaxException {
        List<String> stringList = new ArrayList<>();
        stringList.add("name");
        stringList.add("test");
        stringList.add("age");
        stringList.add("456");

//        Map<String, Object> paramsMap = ImmutableMap.of("name", "file", "format", "csv", "type", "test");
//
//        for (Map.Entry<String, Object> entry : paramsMap.entrySet()) {
//            multipartEntityBuilder.addTextBody(entry.getKey(), String.valueOf(entry.getValue()), ContentType.create("text/plain", Charset.forName("UTF-8")));
//        }
        URL ul = new URL("http://192.168.45.102:9098/hos/knowledge_base_hos_bucket/start.txt");
        URI uri = new URI(ul.getProtocol(), null, ul.getHost(), ul.getPort(), ul.getPath(), ul.getQuery(), null);
        HttpPut httpost = new HttpPut(uri);
        httpost.setHeader("token", "5cfd8d289c274d9c93bc3429410b951f");

        CloseableHttpResponse response = null;
        CloseableHttpClient httpClient = getHttpClient();
        response = httpClient.execute(httpost);
        int statusCode = response.getStatusLine().getStatusCode();
        // 获取响应实体
        HttpEntity entity = response.getEntity();
        // 获取响应信息
        String msg = EntityUtils.toString(entity, "UTF-8");
        System.err.println(statusCode);
    }

    @Test
    public void paraseFormatTest() {
        String dateParam = "${now()-7DAY}";

//        LinkedHashMap<String, String> paramMap = new LinkedHashMap<>();
//        paramMap.put("now()", DateUtil.format(new Date(),"yyyy-MM-dd'T'HH:mm:ss"));
//        String format = StrUtil.format(dateParam, paramMap);
        String regex = "\\$\\{now\\(\\)\\-\\d+DAY\\}";
        ;

//        String format = ReUtil.replaceAll(dateParam, regex, "$1");
        boolean match = ReUtil.isMatch(regex, dateParam);
        System.err.println(match);


    }

    @Test
    public void parseTest() {
        String input = "The current${now()} date is ${now()-5DAY}.${now()-6DAY} ${now()-7DAY} dd${now()-7DAY}d";
        String dayRegex = "\\$\\{now\\(\\)-\\d+DAY\\}";
        String nowRegex = "\\$\\{now\\(\\)\\}";
        String result = ReUtil.replaceAll(input, dayRegex, match -> {
            int days = ReUtil.getFirstNumber(match.group());
            return DateUtil.format(DateUtil.offsetDay(DateUtil.date(), -days), "yyy-MM-dd'T'HH:mm:ss");
        });

        result = ReUtil.replaceAll(result, nowRegex, DateUtil.format(DateUtil.date(), "yyy-MM-dd'T'HH:mm:ss")
        );

        System.out.println("Processed string: " + result);
    }

    public static void main(String[] args) {

        String konwledgeId = DigestUtil.md5Hex16(1 + "cn_ipv6_location_built_in");
        System.err.println(konwledgeId);
    }
}