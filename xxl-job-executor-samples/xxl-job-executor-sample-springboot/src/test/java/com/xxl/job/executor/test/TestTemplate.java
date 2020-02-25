package com.xxl.job.executor.test;

import com.xxl.job.executor.XxlJobExecutorApplication;
import com.xxl.job.executor.service.modelService.ModelJobServcie;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes={XxlJobExecutorApplication.class})
public class TestTemplate {

    @Autowired
    ModelJobServcie modelJobServcie;
    
    @Test
    public void testDemo(){
        //        querySql = "select * from policy_event_log limit 1 FORMAT JSON";
    String querySql = "SELECT common_client_ip FROM tsg_galaxy_v3.connection_record_log LIMIT 2 FORMAT JSON";

        Object data = modelJobServcie.queryClickHouse(querySql);
        System.err.println(data);
        modelJobServcie.pushToKafka("testTopic-test",data.toString());
    }
    @KafkaListener(topics = {"testTopic-test"})
    public void receiveMessage(String message){
        //收到通道的消息之后执行秒杀操作
        System.err.println("成功获取topic "+message);
    }
}
