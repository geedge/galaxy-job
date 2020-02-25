package com.xxl.job.executor.service.modelService;

import com.alibaba.fastjson.JSONObject;
import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.executor.common.config.clickhouse.ClickHouseHttpSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
public class ModelJobServiceImpl extends BaseService implements ModelJobServcie {
	
    @Autowired
    private ClickHouseHttpSource clickHouseHttpSource;
    @Autowired
    private KafkaTemplate kafkaTemplate;
    /**
     * 查询click house
     *
     * @param url
     * @param querySql
     * @return
     */
    @Override
    public Object queryClickHouse(String querySql) {
    	String url = "http://" + clickHouseHttpSource.getUrl();
        Map<String, Object> paramMap = new HashMap();
        paramMap.put("user", clickHouseHttpSource.getUsername());
        paramMap.put("password", clickHouseHttpSource.getPassword());
        paramMap.put("database", clickHouseHttpSource.getDbname());
        paramMap.put("query", querySql);
        JSONObject resultBody = this.httpGet(url, paramMap);
        XxlJobLogger.log("查询到数据为："+resultBody);
        return resultBody.get("data");
    }

    /**
     * kafka生产者
     */
    @Override
    public void pushToKafka(String topic, String message) {
        try {
        	XxlJobLogger.log("kafka 推送消息为  topic ："+topic +" ,message:"+message);
            ListenableFuture<SendResult<String, String>> listenableFuture = kafkaTemplate.send(topic, message);
            SendResult<String, String> sendResult = listenableFuture.get();
            listenableFuture.addCallback(SuccessCallback -> {
                        log.info("kafka Producer发送消息成功！topic=" + sendResult.getRecordMetadata().topic() + ",partition" + sendResult.getRecordMetadata().partition() + ",offset=" + sendResult.getRecordMetadata().offset());
                    },
                    FailureCallback -> log.error("kafka Producer发送消息失败！sendResult=" + JSONObject.toJSONString(sendResult.getProducerRecord())));
            XxlJobLogger.log("kafka 推送消息为  topic ："+topic +" ,message:"+message);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("kafka Producer发送消息失败！,{}", e.getMessage());
            XxlJobLogger.log("kafka 推送消息失败："+e);
        }
    }

}
