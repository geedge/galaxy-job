package com.xxl.job.executor;

import com.xxl.job.executor.core.utils.HttpClientUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListenerAnnotationBeanPostProcessor;

/**
 * @author
 */
@SpringBootApplication
public class XxlJobExecutorApplication {

	public static void main(String[] args) {
        SpringApplication.run(XxlJobExecutorApplication.class, args);

	}

}