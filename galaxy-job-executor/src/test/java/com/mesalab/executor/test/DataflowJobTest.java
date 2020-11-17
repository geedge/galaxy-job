package com.mesalab.executor.test;

import com.mesalab.executor.GalaxyExecutorApplication;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.core.utils.KafkaUtils;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { GalaxyExecutorApplication.class })
public class DataflowJobTest  {

	@Test
	public void topNQueryDruidTest() {
		String serveraddr = "";
		// querySql = "select * from policy_event_log limit 1 FORMAT JSON";
//		String querySql  = "SELECT common_client_ip FROM tsg_galaxy_v3.connection_record_log LIMIT 2 FORMAT JSON";
		String querySql = "select TIME_FORMAT(TIME_FLOOR(__time,'PT5M'),'yyyy-MM-dd HH:mm:ss') as start_time, sum(hits) as hits from security_event_hits_log where policy_id<10 group by TIME_FORMAT(TIME_FLOOR(__time,'PT5M'),'yyyy-MM-dd HH:mm:ss') limit 5";
		String result = HttpClientUtils.httpPost(serveraddr, JsonMapper.toJsonString(querySql));
		
		List<Object> allResults = (List<Object>) JsonMapper.fromJsonString(result, Object.class);
		allResults.forEach(a->System.err.println(a));
		 if(StringUtil.isNotEmpty(allResults)&&allResults.size()>0) {
			 System.err.println(JsonMapper.toJsonString(allResults));
			 try {
				new KafkaUtils().sendMessage("TOP-INTERNAL-HOST-LOG", allResults);
			} catch (Exception e) {
				e.printStackTrace();
			}
		 }
	}
	@Test
	public void getHttpTest() {
		String serversql = "";
		String resultMessage = HttpClientUtils.httpGet(serversql);
		
			if(StringUtil.isNotEmpty(resultMessage)) {
				new KafkaUtils().sendMessage("topic", resultMessage);
			}
	}

//    @KafkaListener(topics = {"TOP-INTERNAL-HOST-LOG"})
	public void receiveMessage(String message) {
		// 收到通道的消息之后执行秒杀操作
		System.err.println("成功获取topic " + message);
	}
}
