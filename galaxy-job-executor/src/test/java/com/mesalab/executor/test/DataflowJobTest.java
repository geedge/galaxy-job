package com.mesalab.executor.test;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.geedgenetworks.utils.JsonMapper;
import com.geedgenetworks.utils.StringUtil;
import com.mesalab.executor.GalaxyExecutorApplication;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.core.utils.KafkaUtils;
import com.mesalab.executor.exception.BusinessException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { GalaxyExecutorApplication.class })
public class DataflowJobTest  {

	private static final Log log = LogFactory.get();

	@Test
	public void topNQueryDruidTest() {
		String url = "/druid/v2/sql";
		// querySql = "select * from policy_event_log limit 1 FORMAT JSON";
//		String querySql  = "SELECT common_client_ip FROM tsg_galaxy_v3.connection_record_log LIMIT 2 FORMAT JSON";
		String querySql = "select TIME_FORMAT(TIME_FLOOR(__time,'PT5M'),'yyyy-MM-dd HH:mm:ss') as start_time, sum(hits) as hits from security_event_hits_log where policy_id<10 group by TIME_FORMAT(TIME_FLOOR(__time,'PT5M'),'yyyy-MM-dd HH:mm:ss') limit 5";
		String result = HttpClientUtils.httpPost(url, JsonMapper.toJsonString(querySql));
		
		List<Object> allResults = (List<Object>) JsonMapper.fromJsonString(result, Object.class);
		allResults.forEach(a->log.error(String.valueOf(a)));
		 if(StringUtil.isNotEmpty(allResults)&&allResults.size()>0) {
			 log.info(JsonMapper.toJsonString(allResults));
			 try {
				new KafkaUtils().sendMessage("TOP-INTERNAL-HOST-LOG", allResults);
			} catch (BusinessException e) {
				e.printStackTrace();
			}
		 }
	}
	@Test
	public void getHttpTest() {
		String url = "/?user=default&password=111111&database=tsg_galaxy&query=select * from policy_event_log limit 1 FORMAT JSON";
		String resultMessage = HttpClientUtils.httpGet(url);
		
			if(StringUtil.isNotEmpty(resultMessage)) {
				new KafkaUtils().sendMessage("topic", resultMessage);
			}
	}

//    @KafkaListener(topics = {"TOP-INTERNAL-HOST-LOG"})
	public void receiveMessage(String message) {
		// 收到通道的消息之后执行秒杀操作
		log.info("成功获取topic " + message);
	}
}
