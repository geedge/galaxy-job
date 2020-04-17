package com.xxl.job.executor.test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.xxl.job.executor.XxlJobExecutorApplication;
import com.xxl.job.executor.core.utils.HttpClientUtils;
import com.xxl.job.executor.core.utils.KafkaUtils;
import com.zdjizhi.utils.DateUtils;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.TimeConstants;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { XxlJobExecutorApplication.class })
public class DeleteDataTest {
	
//	@Test
	public void deleteLogTest() {
//		String url = "http://192.168.40.224:8123/?user=default&password=111111&database=tsg_galaxy&query=select * from policy_event_log limit 1 FORMAT JSON";
//		String resultMessage = HttpClientUtils.httpGet(url);
		System.err.println(JsonMapper.toJsonString(null));
		System.err.println(DateUtils.getSomeDate(TimeConstants.YYYY_MM_DD, -Integer.valueOf(1)));
		String newJosn = "{" + 
				"	\"表单数据\": {" + 
				"		\"jobGroup\": \"2\"," + 
				"		\"jobDesc\": \"清空druid\"," + 
				"		\"executorRouteStrategy\": \"FIRST\"," + 
				"		\"cronGen_display\": \"*+*+*+*+1/1+?\"," + 
				"		\"jobCron\": \"*+*+*+*+1/1+?\"," + 
				"		\"executorHandler\": \"deleteAllReportAndMetricsDataJobHandler\"," + 
				"		\"executorBlockStrategy\": \"SERIAL_EXECUTION\"," + 
				"		\"childJobId\": \"\"," + 
				"		\"executorTimeout\": \"0\"," + 
				"		\"executorFailRetryCount\": \"0\"," + 
				"		\"author\": \"zz\"," + 
				"		\"alarmEmail\": \"\"," + 
				"		\"executorParam\": \"{server:\"192.168.40.224:8081\",datasource:\"\",username:\"\",password:\"\",tablename:\"app_statistic_log\",\"maxdays\":\"30\"}\"," + 
				"		\"id\": \"28\"" + 
				"	}," + 
				"	\"请求有效载荷（payload）\": {" + 
				"		\"EDITOR_CONFIG\": {" + 
				"			\"text\": \"jobGroup=2&jobDesc=%E6%B8%85%E7%A9%BAdruid&executorRouteStrategy=FIRST&cronGen_display=*+*+*+*+1%2F1+%3F&jobCron=*+*+*+*+1%2F1+%3F&executorHandler=deleteAllReportAndMetricsDataJobHandler&executorBlockStrategy=SERIAL_EXECUTION&childJobId=&executorTimeout=0&executorFailRetryCount=0&author=zz&alarmEmail=&executorParam=%7Bserver%3A%22192.168.40.224%3A8081%22%2Cdatasource%3A%22%22%2Cusername%3A%22%22%2Cpassword%3A%22%22%2Ctablename%3A%22app_statistic_log%22%2C%22maxdays%22%3A%2230%22%7D&id=28\"," + 
				"			\"mode\": \"application/json\"" + 
				"		}" + 
				"	}" + 
				"}";
		String httpPost = HttpClientUtils.httpPost("http://192.168.41.22:8080/xxl-job-admin/jobinfo/update", JsonMapper.toJsonString(newJosn));
		System.err.println(httpPost);
	}
	@Test
	public void druidDataInsert() throws ParseException {
		List<Date> dateRange = DateUtils.getDateRange(DateUtils.convertStringToDate("2019-04-18 00:00:00", "yyyy-MM-dd HH:mm:ss"), DateUtils.convertStringToDate("2020-04-14 00:00:00", "yyyy-MM-dd HH:mm:ss"), "PT5M");
		// 时间列表 转为utc
		int count=1;
		SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
		for (Date date : dateRange) {
			String formatDate =sdf.format(date);
			//表循环
//			TOP-EXTERNAL-HOST-LOG {\"destination\":\"47.89.41.97\",\"session_num\":1,\"c2s_byte_num\":0,\"s2c_byte_num\":0,\"c2s_pkt_num\":0,\"s2c_pkt_num\":0,\"stat_time\":\""+formatDate+"\",\"order_by\":\"sessions\"}";
		
//			map.put("destination", "47.89.41.97");
//			map.put("session_num", 1);
//			map.put("c2s_byte_num", 0);
//			map.put("s2c_byte_num", 0);
//			map.put("c2s_pkt_num", 0);
//			map.put("s2c_pkt_num", 0);
//			map.put("stat_time", formatDate);
//			map.put("order_by", "sessions");
	
			count++;
			List<Object> list = Lists.newArrayList();
			List<Object> list2 = Lists.newArrayList();
			for (int i = 0; i < 1000; i++) {
				Map<String,Object> map = Maps.newHashMap();
//				Map<String,Object> map2 = Maps.newHashMap();
				
				
				map.put("destination", 21+"."+22+"."+new Random().nextInt(100)+"."+new Random().nextInt(100));
				map.put("session_num", 1);
				map.put("c2s_byte_num", 0);
				map.put("s2c_byte_num", 0);
				map.put("c2s_pkt_num", 0);
				map.put("s2c_pkt_num", 0);
				map.put("stat_time", formatDate);
				map.put("order_by", "packets");
				//TOP-URLS-LOG {"url":"http://vod.kankan.com/v/90/90606.shtml","session_num":67,"stat_time":"2020-04-16T12:20:00.000+08:00"}
				
//				map.put("session_num", 1);
//				map.put("stat_time", formatDate);
//				TOP-USER-LOG
				
//			    map2.put("session_num", 2);
//			    map2.put("c2s_byte_num", 2);
//			    map2.put("s2c_byte_num", 1);
//			    map2.put("c2s_pkt_num", 36);
//			    map2.put("s2c_pkt_num", 20);
//			    map2.put("stat_time", formatDate);
//			    map2.put("order_by", "sessions");
//				map.put("url", "http://www.google.com/"+count+i+UUID.randomUUID());
//				map2.put("subscriber_id", "czXofx"+count+i+UUID.randomUUID());
//				list.add(map);
				list.add(map);
//				list2.add(map2);
			}
			new KafkaUtils().sendMessage("TOP-EXTERNAL-HOST-LOG",list);
//			new KafkaUtils().sendMessage("TOP-URLS-LOG",list);
//			new KafkaUtils().sendMessage("TOP-USER-LOG",list2);
			if(count%1000==0) {
				System.out.println("size "+dateRange.size());
				System.out.println("count:"+count);
				System.out.println("date:"+formatDate);
			}
		}
		  System.err.println("over");
	}
}
