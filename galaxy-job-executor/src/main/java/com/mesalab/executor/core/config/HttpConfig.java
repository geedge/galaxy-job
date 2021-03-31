package com.mesalab.executor.core.config;

import com.alibaba.nacos.api.config.annotation.NacosValue;
import lombok.Data;
import org.springframework.stereotype.Component;

@Data
@Component
public class HttpConfig {
    /**
     * 最大连接数400
     */
    @NacosValue(value = "${http.pool.max.connection}", autoRefreshed = true)
    private int maxConnectionNum;

    /**
     * 单路由最大连接数80
     */
    @NacosValue(value = "${http.pool.max.per.route}", autoRefreshed = true)
    private int maxPerRoute;

    /**
     * 向服务端请求超时时间设置(单位:毫秒)
     */
    @NacosValue(value = "${http.pool.request.timeout}", autoRefreshed = true)
    private int serverRequestTimeOut;


    /**
     * 向服务端连接超时时间设置(单位:毫秒)
     */
    @NacosValue(value = "${http.pool.connect.timeout}", autoRefreshed = true)
    private int serverConnectTimeOut;

    /**
     * 服务端响应超时时间设置(单位:毫秒)
     */
    @NacosValue(value = "${http.pool.response.timeout}", autoRefreshed = true)
    private int serverResponseTimeOut;

}
