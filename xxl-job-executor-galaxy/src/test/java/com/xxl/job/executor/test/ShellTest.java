package com.xxl.job.executor.test;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.google.common.base.Splitter;
import com.xxl.job.executor.XxlJobExecutorApplication;
import com.xxl.job.executor.core.utils.HttpClientUtils;
@RunWith(SpringRunner.class)
@SpringBootTest(classes = { XxlJobExecutorApplication.class })
public class ShellTest {

	
	
	@Test
	public void shellTest() {
		String str = "{\r\n" + 
				"    \"clientId\": null,\r\n" + 
				"    \"query\": {\r\n" + 
				"        \"queryType\": \"iplearning\",\r\n" + 
				"        \"dataSource\": \"IP_LEARNING_VIEW\",\r\n" + 
				"        \"parameters\": {\r\n" + 
				"            \"match\": [\r\n" + 
				"                {\r\n" + 
				"                    \"type\": \"regex\",\r\n" + 
				"                    \"fieldKey\": \"FQDN_NAME\",\r\n" + 
				"                    \"fieldValues\": [\r\n" + 
				"                        \"360\"\r\n" + 
				"                    ]\r\n" + 
				"                }\r\n" + 
				"            ],\r\n" + 
				"            \"range\": [\r\n" + 
				"                {\r\n" + 
				"                    \"type\": \"ge\",\r\n" + 
				"                    \"fieldKey\": \"PROTOCOL\",\r\n" + 
				"                    \"fieldValues\": [\r\n" + 
				"                        \"TLS\",\r\n" + 
				"                        \"HTTP\"\r\n" + 
				"                    ]\r\n" + 
				"                },\r\n" + 
				"                {\r\n" + 
				"                    \"type\": \"eq\",\r\n" + 
				"                    \"fieldKey\": \"DEPTH\",\r\n" + 
				"                    \"fieldValues\": [\r\n" + 
				"                        \"0..1\"\r\n" + 
				"                    ]\r\n" + 
				"                },\r\n" + 
				"                {\r\n" + 
				"                    \"type\": \"ge\",\r\n" + 
				"                    \"fieldKey\": \"UNIQ_CIP\",\r\n" + 
				"                    \"fieldValues\": [\r\n" + 
				"                        0\r\n" + 
				"                    ]\r\n" + 
				"                }\r\n" + 
				"            ],\r\n" + 
				"            \"intervals\": [\r\n" + 
				"                \"2020-01-01 00:00:00/2020-08-02 00:00:00\"\r\n" + 
				"            ],\r\n" + 
				"            \"limit\": \"3\"\r\n" + 
				"        }\r\n" + 
				"    }\r\n" + 
				"}";
		
		String httpPost = HttpClientUtils.httpPost("http://192.168.40.224:9999/knowledge/v1/?iplearni1ng", str);
		System.out.println(httpPost);
		
	}
}
