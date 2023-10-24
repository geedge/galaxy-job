package com.mesalab.executor.core.utils;

import com.mesalab.executor.exception.BusinessException;
import com.xxl.job.core.log.XxlJobLogger;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.support.ProducerListenerAdapter;
import org.springframework.stereotype.Component;

/**
 * 默认生产者监听器
 */
@Component
public class DefaultProducerListener extends ProducerListenerAdapter {
    /**
     * 发送消息成功后调用
     */
    @Override
    public void onSuccess(String topic, Integer partition, Object key,
                          Object value, RecordMetadata recordMetadata) {
        super.onSuccess(topic, partition, key, value, recordMetadata);
    }

    /**
     * 发送消息错误后调用
     */
    @Override
    public void onError(String topic, Integer partition, Object key,
                        Object value, Exception exception) {
        super.onError(topic, partition, key, value, exception);
        XxlJobLogger.log("topic {}, partition {}, key {}, value {}, exception {}",topic, partition, key, value, exception);
        throw new BusinessException(exception.getMessage());
    }

    /**
     * 是否开启发送监听
     *
     * @return true开启，false关闭
     */
    @Override
    public boolean isInterestedInSuccess() {
        return true;
    }

}