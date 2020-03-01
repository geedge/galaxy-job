package com.xxl.job.executor.core.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Component
public class KafkaUtils {
    private static Logger logger = LoggerFactory.getLogger(KafkaUtils.class);
    @Autowired
    private KafkaTemplate kafkaTemplate;

    public void sendMessage(String topic, String data) {
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

    }

}
