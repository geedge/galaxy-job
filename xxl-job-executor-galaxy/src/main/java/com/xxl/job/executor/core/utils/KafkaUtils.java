package com.xxl.job.executor.core.utils;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;

public class KafkaUtils {
    private static Logger logger = LoggerFactory.getLogger(KafkaUtils.class);
    public KafkaTemplate kafkaTemplate = (KafkaTemplate) SpringContextUtil.getBean("kafkaTemplate");

    public void sendMessage(String topic, String data) {
    	if(StringUtil.isNotBlank(data)) {
    		logger.info("kafka sendMessage start");
    		ListenableFuture<SendResult<Integer, String>> future = kafkaTemplate.send(topic, data);
    		future.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
    			@Override
    			public void onFailure(Throwable ex) {
    				logger.error("kafka sendMessage error, ex = {}, topic = {}, data = {}", ex, topic, data);
    			}
    			
    			@Override
    			public void onSuccess(SendResult<Integer, String> result) {
    				logger.info("kafka sendMessage success topic = {}, data = {}",topic, data);
    			}
    		});
    		logger.info("kafka sendMessage end");
    	} else {
    		logger.info("kafka can not send message when the data is empty ");
    	}

    }
    /**
     * 批量发送
     * @param topic
     * @param data List<String>
     */
    public void sendMessage(String topic, Object data) {
    	if(data instanceof List) {
    		for (Object object : (List) data) {
    			sendMessage(topic, JsonMapper.toJsonString(object));
    		}
    	}else {
    		sendMessage(topic, JsonMapper.toJsonString(data));
    	}
    }

}
