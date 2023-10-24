package com.mesalab.executor.test;

import com.geedgenetworks.utils.DateUtils;
import com.geedgenetworks.utils.JsonMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mesalab.executor.GalaxyExecutorApplication;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.core.utils.KafkaUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.security.SecureRandom;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {GalaxyExecutorApplication.class})
@Slf4j
public class DeleteDataTest {

    //	@Test
    public void deleteLogTest() {
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
                "		\"executorParam\": \"{server:\"192.168.44.12:8081\",datasource:\"\",username:\"\",password:\"\",tablename:\"app_statistic_log\",\"maxdays\":\"30\"}\"," +
                "		\"id\": \"28\"" +
                "	}," +
                "	\"请求有效载荷（payload）\": {" +
                "		\"EDITOR_CONFIG\": {" +
                "			\"text\": \"jobGroup=2&jobDesc=%E6%B8%85%E7%A9%BAdruid&executorRouteStrategy=FIRST&cronGen_display=*+*+*+*+1%2F1+%3F&jobCron=*+*+*+*+1%2F1+%3F&executorHandler=deleteAllReportAndMetricsDataJobHandler&executorBlockStrategy=SERIAL_EXECUTION&childJobId=&executorTimeout=0&executorFailRetryCount=0&author=zz&alarmEmail=&executorParam=%7Bserver%3A%22192.168.44.12%3A8081%22%2Cdatasource%3A%22%22%2Cusername%3A%22%22%2Cpassword%3A%22%22%2Ctablename%3A%22app_statistic_log%22%2C%22maxdays%22%3A%2230%22%7D&id=28\"," +
                "			\"mode\": \"application/json\"" +
                "		}" +
                "	}" +
                "}";
        String httpPost = HttpClientUtils.httpPost("/xxl-job-admin/jobinfo/update", JsonMapper.toJsonString(newJosn));
        log.info(httpPost);
    }

    @Test
    public void druidDataInsert() throws ParseException {
        List<Date> dateRange = DateUtils.getDateRange(DateUtils.convertStringToDate("2019-04-18 00:00:00", "yyyy-MM-dd HH:mm:ss"), DateUtils.convertStringToDate("2020-04-14 00:00:00", "yyyy-MM-dd HH:mm:ss"), "PT5M");
        // 时间列表 转为utc
        int count = 1;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        for (Date date : dateRange) {
            String formatDate = sdf.format(date);
            //表循环
            count++;
            List<Object> list = Lists.newArrayList();
            for (int i = 0; i < 1000; i++) {
                Map<String, Object> map = Maps.newHashMap();

                map.put("destination", 21 + "." + 22 + "." + new SecureRandom().nextInt(100) + "." + new SecureRandom().nextInt(100));
                map.put("session_num", 1);
                map.put("c2s_byte_num", 0);
                map.put("s2c_byte_num", 0);
                map.put("c2s_pkt_num", 0);
                map.put("s2c_pkt_num", 0);
                map.put("stat_time", formatDate);
                map.put("order_by", "packets");
                list.add(map);
            }
            new KafkaUtils().sendMessage("TOP-EXTERNAL-HOST-LOG", list);
            if (count % 1000 == 0) {
                log.info("size " + dateRange.size());
                log.info("count:" + count);
                log.info("date:" + formatDate);
            }
        }
    }
}
