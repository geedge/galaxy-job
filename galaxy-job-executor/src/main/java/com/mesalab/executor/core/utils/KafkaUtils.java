package com.mesalab.executor.core.utils;

import cn.hutool.log.Log;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.List;

public class KafkaUtils {
    private Log logger = Log.get();
    public KafkaTemplate kafkaTemplate = (KafkaTemplate) SpringContextUtil.getBean("kafkaTemplate");

    public void sendMessage(String topic, String data) {
    	if(StringUtil.isNotBlank(data)) {
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
    	} else {
    		logger.info("kafka can not send message when the data is empty ");
    	}

    }
    /**
     * 批量发送,限制数据类型
     * @param topic
     * @param data List<Object> 集合对象
     */
    public void sendMessage(String topic, List<Object> data) {

		for (Object object :  data) {
			sendMessage(topic, JsonMapper.toJsonString(object));
		}

    }

}
