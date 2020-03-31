package com.xxl.job.executor.test;

import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.xxl.job.executor.XxlJobExecutorApplication;
import com.xxl.job.executor.core.config.DeletionConfig;
import com.xxl.job.executor.core.utils.HttpClientUtils;
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
	public void propertiesTest() {
		System.err.println(DeletionConfig.analyticServer);
	}
}
