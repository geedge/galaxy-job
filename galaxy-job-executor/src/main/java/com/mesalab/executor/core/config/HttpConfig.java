package com.mesalab.executor.core.config;

import com.alibaba.nacos.api.config.annotation.NacosValue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


@Component
public class HttpConfig {
    /**
     * 最大连接数400
     */
    @NacosValue(value = "${http.pool.max.connection}",autoRefreshed = true)
    public static int maxConnectionNum;

    /**
     * 单路由最大连接数80
     */
    @NacosValue(value = "${http.pool.max.per.route}",autoRefreshed = true)
    public static int maxPerRoute;

    /**
     * 向服务端请求超时时间设置(单位:毫秒)
     */
    @NacosValue(value = "${http.pool.request.timeout}",autoRefreshed = true)
    public static int serverRequestTimeOut;


    /**
     * 向服务端连接超时时间设置(单位:毫秒)
     */
    @NacosValue(value = "${http.pool.connect.timeout}",autoRefreshed = true)
    public static int serverConnectTimeOut;

    /**
     * 服务端响应超时时间设置(单位:毫秒)
     */
    @NacosValue(value = "${http.pool.response.timeout}",autoRefreshed = true)
    public static int serverResponseTimeOut;

}
