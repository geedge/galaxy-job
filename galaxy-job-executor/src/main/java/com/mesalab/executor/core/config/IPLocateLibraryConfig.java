package com.mesalab.executor.core.config;

import com.alibaba.nacos.api.config.annotation.NacosValue;
import lombok.Data;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
public class IPLocateLibraryConfig {
    @NacosValue(value = "${xxl.job.executor.ip.address}")
    private String address;
    @NacosValue(value = "${xxl.job.executor.ip.username}")
    private String username;
    @NacosValue(value = "${xxl.job.executor.ip.password}")
    private String password;
}
