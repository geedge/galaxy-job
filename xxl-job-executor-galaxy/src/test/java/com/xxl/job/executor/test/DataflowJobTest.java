package com.xxl.job.executor.test;

import com.google.common.collect.Maps;
import com.xxl.job.executor.XxlJobExecutorApplication;
import com.xxl.job.executor.core.utils.HttpClientUtils;
import com.xxl.job.executor.core.utils.KafkaUtils;
import com.xxl.job.executor.pojo.DruidQueryParam;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;


import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { XxlJobExecutorApplication.class })
public class DataflowJobTest<E> {

	@Test
	public void topNQueryDruidTest() {
		String url = "http://192.168.40.224:8089/druid/v2/sql";
		// querySql = "select * from policy_event_log limit 1 FORMAT JSON";
//		 String querySql  = "SELECT common_client_ip FROM tsg_galaxy_v3.connection_record_log LIMIT 2 FORMAT JSON";
		String querySql = "select TIME_FORMAT(TIME_FLOOR(__time,'PT5M'),'yyyy-MM-dd HH:mm:ss') as start_time, sum(hits) as hits from security_event_hits_log where policy_id<10 group by TIME_FORMAT(TIME_FLOOR(__time,'PT5M'),'yyyy-MM-dd HH:mm:ss') limit 5";
//		String querySql = "SELECT common_client_ip AS source,sum(session_num) AS session_num,sum( c2s_byte_num ) AS c2s_byte_num,sum( s2c_byte_num ) AS s2c_byte_num,sum(c2s_pkt_num) AS c2s_pkt_num,sum(s2c_pkt_num) AS s2c_pkt_num,TIME_FLOOR(CURRENT_TIMESTAMP,'PT5M') as \"time\",'sessions' as order_by FROM top_internal_host_log_hot WHERE __time >= TIME_FLOOR(CURRENT_TIMESTAMP,'PT5M') AND __time < TIME_FLOOR(CURRENT_TIMESTAMP,'PT5M') GROUP BY common_client_ip limit 5";
		Map<String, String> context = Maps.newHashMap();
		context.put("sqlTimeZone", "Asia/Shanghai");
		context.put("skipEmptyBuckets", "false");
		DruidQueryParam druidQueryParam = DruidQueryParam.builder().context(context).query(querySql)
				.resultFormat("object").build();

		Map<String, String> resultMap = HttpClientUtils.httpPost(url, JsonMapper.toJsonString(druidQueryParam));
		
		List<Object> allResults = (List<Object>) JsonMapper.fromJsonString(resultMap.get("result"), Object.class);
		allResults.forEach(a->System.err.println(a));
		 if(StringUtil.isNotEmpty(allResults)&&allResults.size()>0) {
			 System.err.println(JsonMapper.toJsonString(allResults));
			 try {
				new KafkaUtils().sendMessage("TOP-INTERNAL-HOST-LOG", allResults);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		 }
	}
	@Test
	public void getHttpTest() {
		String url = "http://192.168.40.194:8123/?user=default&password=111111&database=tsg_galaxy&query=select * from policy_event_log limit 1 FORMAT JSON";
		String resultMessage = HttpClientUtils.httpGet(url);
		Map resultMap = (Map)JsonMapper.fromJsonString(resultMessage, Object.class);
		
		if ("-1".equals(resultMessage)) {
		} else {
			if(StringUtil.isNotEmpty(resultMessage)) {
//				new KafkaUtils().sendMessage("topic", resultMessage);
			}
			System.out.print("推送kafka消息:"+resultMap.get("data"));
		}
	}

//    @KafkaListener(topics = {"TOP-INTERNAL-HOST-LOG"})
	public void receiveMessage(String message) {
		// 收到通道的消息之后执行秒杀操作
		System.err.println("成功获取topic " + message);
	}
	@Test
	public void jsonFormatTest() {
		String json = "{\"url\":\"http://192.168.40.224:8089/druid/v2/sql\",\"query\":\"SELECT common_client_ip AS source,sum( session_num ) AS session_num,sum( c2s_byte_num ) AS c2s_byte_num,sum( s2c_byte_num ) AS s2c_byte_num,sum( c2s_pkt_num ) AS c2s_pkt_num,sum( s2c_pkt_num ) AS s2c_pkt_num,TIME_FLOOR(CURRENT_TIMESTAMP,'PT5M') as 'time','sessions' as order_by FROM top_internal_host_log_hot WHERE __time >= TIME_FLOOR(CURRENT_TIMESTAMP,'PT5M') AND __time < TIME_FLOOR(CURRENT_TIMESTAMP,'PT5M') GROUP BY common_client\",\"topic\":\"TOP-INTERNAL-HOST-LOG\"}";
//		String jsonString = JsonMapper.toJsonString(json);
//		System.err.println(jsonString);
			Object fromJsonString = JsonMapper.fromJsonString(json, Object.class);
			System.err.println(fromJsonString);
	}
	public static void main(String[] args) {
//		assertEquals(StringUtil.isAnyEmpty("url","method","d"), true);
		String json = "{\"url\":\"http://192.168.40.224:8089/druid/v2/sql\",\"query\":\"SELECT common_client_ip AS source,sum( session_num ) AS session_num,sum( c2s_byte_num ) AS c2s_byte_num,sum( s2c_byte_num ) AS s2c_byte_num,sum( c2s_pkt_num ) AS c2s_pkt_num,sum( s2c_pkt_num ) AS s2c_pkt_num,TIME_FLOOR(CURRENT_TIMESTAMP,'PT5M') as 'time','sessions' as order_by FROM top_internal_host_log_hot WHERE __time >= TIME_FLOOR(CURRENT_TIMESTAMP,'PT5M') AND __time < TIME_FLOOR(CURRENT_TIMESTAMP,'PT5M') GROUP BY common_client\",\"topic\":\"TOP-INTERNAL-HOST-LOG\"}";
//		String jsonString = JsonMapper.toJsonString(json);
//		System.err.println(jsonString);
			Map<String,String> fromJsonString = Maps.newHashMap();
			try {
				fromJsonString = (Map) JsonMapper.fromJsonString(json, Map.class);
				System.err.println(fromJsonString);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println(fromJsonString);
	}
}
