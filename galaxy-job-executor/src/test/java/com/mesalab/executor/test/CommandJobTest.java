package com.mesalab.executor.test;

import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.TypeUtil;
import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSONPath;
import com.alibaba.fastjson.util.TypeUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.exception.BusinessException;
import com.mesalab.executor.pojo.IpLocateLibraryParam;
import com.ning.http.client.multipart.FilePart;
import com.ning.http.client.multipart.MultipartBody;
import com.ning.http.client.multipart.Part;
import com.ning.http.client.providers.jdk.MultipartRequestEntity;
import com.zdjizhi.utils.IpLookupV2;
import org.apache.http.Header;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mesalab.executor.core.utils.HttpClientUtils.getHttpClient;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class CommandJobTest {

    @Test
    public void getIPMMDBFile() {
        IpLookupV2 ipLookup = new IpLookupV2.Builder(false)
                .loadDataFileV4("C:\\code\\galaxy-qgw-service\\dat\\ip_v4_built_in.mmdb")
                .loadDataFileV6("C:\\code\\galaxy-qgw-service\\dat\\ip_v6_built_in.mmdb")
                .loadDataFilePrivateV4("C:\\code\\galaxy-qgw-service\\dat\\ip_v6_built_in.mmdb")
                .loadDataFilePrivateV6("")
                .build();

        String latLngLookup = ipLookup.latLngLookup("1.0.0.1");

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
        String resultStr = HttpClientUtils.httpPostFile(knowledgeServer,file , paramsMap);

        if(!"-1".equals(resultStr)){
            Map result = JSONUtil.toBean(resultStr, Map.class);
            Assert.assertTrue("success",result.get("status")!=null && "200".equals(String.valueOf(result.get("status"))));
        }

    }
    @Test
    public void getIPMMDBInfo() {
//        String result = "{\"status\":200,\"code\":\"200666\",\"queryKey\":null,\"success\":true,\"message\":\"Success\",\"statistics\":null,\"job\":null,\"formatType\":null,\"meta\":null,\"data\":[{\"id\":\"acf1db8589c5e277-ab61b6eaadcbbc28\",\"name\":\"ip_v4_built_in\",\"type\":\"ip_location\",\"path\":\"http://192.168.45.102:9098/hos/knowledge_base_hos_bucket/45e5e93a-87ef-4246-81a7-87ac0ed3e5d9-aXBfdjRfYnVpbHRfaW4=.mmdb\",\"size\":621,\"format\":\"mmdb\",\"sha256\":\"894c3771466d7f58ef7d2ff0566088e067a5f73b469a0f3a5057a4e80476e472\",\"origin_url\":null,\"version\":\"221116_2\",\"updateTime\":\"2022-11-16T10:30:001\"},{\"id\":\"acf1db8589c5e277-ab61b6eaadcbbc28\",\"name\":\"ip_v4_built_in\",\"type\":\"ip_location\",\"path\":\"http://192.168.45.102:9098/hos/knowledge_base_hos_bucket/45e5e93a-87ef-4246-81a7-87ac0ed3e5d9-aXBfdjRfYnVpbHRfaW4=.mmdb\",\"size\":621,\"format\":\"mmdb\",\"sha256\":\"894c3771466d7f58ef7d2ff0566088e067a5f73b469a0f3a5057a4e80476e472\",\"origin_url\":null,\"version\":\"latest\",\"updateTime\":\"2022-11-16T10:28:007\"},{\"id\":\"acf1db8589c5e277-4afada9d4f3ce14d\",\"name\":\"ip_v4_built_in\",\"type\":\"ip_location\",\"path\":\"http://192.168.45.102:9098/hos/knowledge_base_hos_bucket/374c286b-bad4-4d46-ae8e-b77f82b39096-aXBfdjRfYnVpbHRfaW4=.mmdb\",\"size\":621,\"format\":\"mmdb\",\"sha256\":\"64bc682ecf08011e59adf69bc7ab2b904a40c418a17abc2d7c75d6c70ad9aac0\",\"origin_url\":null,\"version\":\"221116_1\",\"updateTime\":\"2022-11-16T10:28:007\"},{\"id\":\"acf1db8589c5e277-6a12660a3f4a1f74\",\"name\":\"ip_v6_user_defined\",\"type\":\"ip_location\",\"path\":\"http://192.168.45.102:9098/hos/knowledge_base_hos_bucket/20537de3-f94e-43e2-960e-d1a0230244c0-aXBfdjRfYnVpbHRfaW4=.mmdb\",\"size\":29245096,\"format\":\"mmdb\",\"sha256\":\"34d928289f8ef01adc1d365da24d837fa0da9e114afed5b08262119c797bbfbb\",\"origin_url\":null,\"version\":\"221116_9\",\"updateTime\":\"2022-11-16T07:55:055\"},{\"id\":\"acf1db8589c5e277-80fa25f30e160eb3\",\"name\":\"ip_v6_user_defined\",\"type\":\"ip_location\",\"path\":\"http://192.168.45.102:9098/hos/knowledge_base_hos_bucket/616b5520-d87f-4020-a700-984a0a7fa3e0-aXBfdjRfYnVpbHRfaW4=.mmdb\",\"size\":29245096,\"format\":\"mmdb\",\"sha256\":\"34d928289f8ef01adc1d365da24d837fa0da9e114afed5b08262119c797bbfbb\",\"origin_url\":null,\"version\":\"221116_8\",\"updateTime\":\"2022-11-16T07:52:053\"},{\"id\":\"acf1db8589c5e277-354d70dbc349d600\",\"name\":\"ip_v6_user_defined\",\"type\":\"ip_location\",\"path\":\"http://192.168.45.102:9098/hos/knowledge_base_hos_bucket/6146ab05-cdac-4cc3-94dd-ca8e2bdcbc95-aXBfdjRfYnVpbHRfaW4=.mmdb\",\"size\":29245096,\"format\":\"mmdb\",\"sha256\":\"34d928289f8ef01adc1d365da24d837fa0da9e114afed5b08262119c797bbfbb\",\"origin_url\":null,\"version\":\"221116_7\",\"updateTime\":\"2022-11-16T07:50:010\"},{\"id\":\"acf1db8589c5e277-81f242f63113ff41\",\"name\":\"ip_v6_user_defined\",\"type\":\"ip_location\",\"path\":\"http://192.168.45.102:9098/hos/knowledge_base_hos_bucket/28ea3cd5-70fd-4244-864d-0cd81dfcfe5c-aXBfdjRfYnVpbHRfaW4=.mmdb\",\"size\":29245096,\"format\":\"mmdb\",\"sha256\":\"34d928289f8ef01adc1d365da24d837fa0da9e114afed5b08262119c797bbfbb\",\"origin_url\":null,\"version\":\"221116_6\",\"updateTime\":\"2022-11-16T07:47:033\"},{\"id\":\"acf1db8589c5e277-13e586a4bc1084e4\",\"name\":\"ip_v6_user_defined\",\"type\":\"ip_location\",\"path\":\"http://192.168.45.102:9098/hos/knowledge_base_hos_bucket/bb46a4e2-6a6d-4640-94c5-534021474750-aXBfdjRfYnVpbHRfaW4=.mmdb\",\"size\":29245096,\"format\":\"mmdb\",\"sha256\":\"34d928289f8ef01adc1d365da24d837fa0da9e114afed5b08262119c797bbfbb\",\"origin_url\":null,\"version\":\"221116_5\",\"updateTime\":\"2022-11-16T07:19:004\"},{\"id\":\"acf1db8589c5e277-99108ffd2c29c953\",\"name\":\"ip_v6_user_defined\",\"type\":\"ip_location\",\"path\":\"http://192.168.45.102:9098/hos/knowledge_base_hos_bucket/aff2fc7c-b8f1-425d-8944-b8a2579fb6df-aXBfdjRfYnVpbHRfaW4=.mmdb\",\"size\":29245096,\"format\":\"mmdb\",\"sha256\":\"34d928289f8ef01adc1d365da24d837fa0da9e114afed5b08262119c797bbfbb\",\"origin_url\":null,\"version\":\"221116_4\",\"updateTime\":\"2022-11-16T03:32:024\"},{\"id\":\"acf1db8589c5e277-f38165eaffc9990c\",\"name\":\"ip_v6_user_defined\",\"type\":\"ip_location\",\"path\":\"http://192.168.45.102:9098/hos/knowledge_base_hos_bucket/d8ebdc25-6534-47b5-8718-55b03bc32d99-aXBfdjRfYnVpbHRfaW4=.mmdb\",\"size\":29245096,\"format\":\"mmdb\",\"sha256\":\"34d928289f8ef01adc1d365da24d837fa0da9e114afed5b08262119c797bbfbb\",\"origin_url\":null,\"version\":\"221116_3\",\"updateTime\":\"2022-11-16T03:31:016\"},{\"id\":\"acf1db8589c5e277-ac418a5395b94965\",\"name\":\"ip_v6_user_defined\",\"type\":\"ip_location\",\"path\":\"http://192.168.45.102:9098/hos/knowledge_base_hos_bucket/2fb51f4c-6004-44e3-a41a-b999a96bab8d-aXBfdjRfYnVpbHRfaW4=.mmdb\",\"size\":29245096,\"format\":\"mmdb\",\"sha256\":\"34d928289f8ef01adc1d365da24d837fa0da9e114afed5b08262119c797bbfbb\",\"origin_url\":null,\"version\":\"221116_2\",\"updateTime\":\"2022-11-16T03:24:055\"},{\"id\":\"acf1db8589c5e277-c49a42b0dbcbdb84\",\"name\":\"ip_v6_user_defined\",\"type\":\"ip_location\",\"path\":\"http://192.168.45.102:9098/hos/knowledge_base_hos_bucket/bca726aa-bd04-48e3-924b-b8ec55afb18f-aXBfdjRfYnVpbHRfaW4=.mmdb\",\"size\":29245096,\"format\":\"mmdb\",\"sha256\":\"34d928289f8ef01adc1d365da24d837fa0da9e114afed5b08262119c797bbfbb\",\"origin_url\":null,\"version\":\"221116_1\",\"updateTime\":\"2022-11-16T03:19:014\"},{\"id\":\"acf1db8589c5e277-6a12660a3f4a1f74\",\"name\":\"ip_v6_user_defined\",\"type\":\"ip_location\",\"path\":\"http://192.168.45.102:9098/hos/knowledge_base_hos_bucket/20537de3-f94e-43e2-960e-d1a0230244c0-aXBfdjRfYnVpbHRfaW4=.mmdb\",\"size\":29245096,\"format\":\"mmdb\",\"sha256\":\"34d928289f8ef01adc1d365da24d837fa0da9e114afed5b08262119c797bbfbb\",\"origin_url\":null,\"version\":\"latest\",\"updateTime\":\"2022-11-15T10:07:008\"},{\"id\":\"acf1db8589c5e277-c35f73d9448e33d8\",\"name\":\"ip_v6_user_defined\",\"type\":\"ip_location\",\"path\":\"http://192.168.45.102:9098/hos/knowledge_base_hos_bucket/fb1350b7-bed0-4912-ba47-74004405a229-aXBfdjRfYnVpbHRfaW4=.mmdb\",\"size\":29245096,\"format\":\"mmdb\",\"sha256\":\"34d928289f8ef01adc1d365da24d837fa0da9e114afed5b08262119c797bbfbb\",\"origin_url\":null,\"version\":\"221115_1\",\"updateTime\":\"2022-11-15T10:07:008\"},{\"id\":\"7ce2f9890950ba90-09ad5a50fbf3bb90\",\"name\":\"asn_v4\",\"type\":\"asn\",\"path\":\"http://192.168.45.102:9098/hos/knowledge_base_hos_bucket/f6e96acc-6544-4027-a941-61c64f449014-YXNuX3Y0.mmdb\",\"size\":3659771,\"format\":\"mmdb\",\"sha256\":\"379c7d2fa50ca9ca4e892f552149ae3d3d952459750c0fa0a7632c7eb1935a45\",\"origin_url\":null,\"version\":\"latest\",\"updateTime\":\"2022-11-15T09:58:028\"},{\"id\":\"7ce2f9890950ba90-09ad5a50fbf3bb90\",\"name\":\"asn_v4\",\"type\":\"asn\",\"path\":\"http://192.168.45.102:9098/hos/knowledge_base_hos_bucket/f6e96acc-6544-4027-a941-61c64f449014-YXNuX3Y0.mmdb\",\"size\":3659771,\"format\":\"mmdb\",\"sha256\":\"379c7d2fa50ca9ca4e892f552149ae3d3d952459750c0fa0a7632c7eb1935a45\",\"origin_url\":null,\"version\":\"221115_1\",\"updateTime\":\"2022-11-15T09:58:027\"}]}\n";
        String knowledgeServer = "http://192.168.45.102:9999/knowledge_base/v1";
        String result = HttpClientUtils.httpGet(knowledgeServer);
        Object extract = JSONPath.extract(result, "$.data[version=='latest']");
        JSONArray objects = JSONUtil.parseArray(extract);
        List<IpLocateLibraryParam.KnowledgeBase> infos = JSONUtil.toList(objects, IpLocateLibraryParam.KnowledgeBase.class);

        Map<String, String> historyInfo = new HashMap<>();
        for (IpLocateLibraryParam.KnowledgeBase info : infos) {
            historyInfo.put(info.getName(), info.getSha256());
        }
        System.err.println(historyInfo);

        IpLocateLibraryParam ipLocateLibraryParam = new IpLocateLibraryParam();
        IpLocateLibraryParam.KnowledgeBase k1 = new IpLocateLibraryParam.KnowledgeBase();
        k1.setName("ip_v4_built_in");
        k1.setSha256("7e37140a851369b64ed219f9d1a9fb859e9b7c8d446568e4f934aea34b01b3e1");
        IpLocateLibraryParam.KnowledgeBase k2 = new IpLocateLibraryParam.KnowledgeBase();
        k2.setName("ip_v6_built_in");
        k2.setSha256("7e37140a851369b64ed219f9d1a9fb859e9b7c8d446568e4f934aea34b01b3e1");

        ipLocateLibraryParam.setKnowledgeBaseList(Arrays.asList(k1, k2));
        for (IpLocateLibraryParam.KnowledgeBase knowledgeBase : ipLocateLibraryParam.getKnowledgeBaseList()) {

            System.err.println(knowledgeBase.getName());
            System.err.println(historyInfo.get(knowledgeBase.getName()));
        }
        System.err.println(extract);



    }
}