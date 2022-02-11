package com.mesalab.executor.core.config;

import com.alibaba.nacos.api.config.annotation.NacosValue;
import lombok.Data;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
public class IPLocateLibraryConfig {
    @NacosValue(value = "${ip.locate.library.server}")
    private String server;
    @NacosValue(value = "${ip.locate.library.username}")
    private String username;
    @NacosValue(value = "${ip.locate.library.password}")
    private String password;
}
