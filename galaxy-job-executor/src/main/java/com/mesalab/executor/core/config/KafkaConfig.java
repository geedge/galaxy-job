package com.mesalab.executor.core.config;
import com.alibaba.nacos.api.config.annotation.NacosValue;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {

	@NacosValue(value = "${spring.kafka.bootstrap-servers}",autoRefreshed = true)
	private String servers;

    @NacosValue(value = "${spring.kafka.ssl.enable}",autoRefreshed = true)
    private String enable;
    @NacosValue(value = "${spring.kafka.ssl.username}")
    private String username;
    @NacosValue(value = "${spring.kafka.ssl.pin}")
    private String pin;

    @Bean
    public Map<String,Object> producerConfigs(){
    	Map<String,Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,servers);
        configs.put(ProducerConfig.RETRIES_CONFIG, 1);
        configs.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        configs.put(ProducerConfig.ACKS_CONFIG, "1");
        configs.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 524288);
        //设置序列化
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        if(Boolean.parseBoolean(enable)){
            configs.put("security.protocol", "SASL_PLAINTEXT");
            configs.put("sasl.mechanism", "PLAIN");
            configs.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='"+username+"' password='"+pin+"';");
        }

        //设置自定义分区
        return configs;
    }
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        return new DefaultKafkaProducerFactory(producerConfigs());
    }
 
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
