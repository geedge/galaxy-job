package com.mesalab.executor.core.config;

import com.alibaba.nacos.api.PropertyKeyConst;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Getter
@Component
public class NacosConfig {

    @Value("${nacos.config.server-addr}")
    private String serverAddr;
    @Value("${nacos.config.username}")
    private String username;
    @Value("${nacos.config.password}")
    private String password;
    @Value("${nacos.config.namespace}")
    private String namespace;
    @Value("${nacos.config.group}")
    private String group;

    public Properties getProperties() {
        Properties properties = new Properties();
        properties.put(PropertyKeyConst.SERVER_ADDR, serverAddr);
        properties.put(PropertyKeyConst.USERNAME, username);
        properties.put(PropertyKeyConst.PASSWORD, password);
        properties.put(PropertyKeyConst.NAMESPACE, namespace);
        return properties;
    }
}
