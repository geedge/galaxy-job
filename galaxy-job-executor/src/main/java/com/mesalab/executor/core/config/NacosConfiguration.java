package com.mesalab.executor.core.config;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class NacosConfiguration {

    private static final Log log = LogFactory.get();

    @Autowired
    private NacosConfig nacosConfig;

    @Bean(name = "configService")
    public ConfigService getConfigService() {
        ConfigService configService = null;
        try {
            configService = NacosFactory.createConfigService(nacosConfig.getProperties());
        } catch (NacosException e) {
            log.error("NacosException:{}", e);
        }
        return configService;
    }
}