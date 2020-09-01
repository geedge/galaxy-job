package com.mesalab.executor.core.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


@Component
public class HttpConfig {
    /**
     * 最大连接数400
     */
    public static int maxConnectionNum;

    /**
     * 单路由最大连接数80
     */

    public static int maxPerRoute;

    /**
     * 向服务端请求超时时间设置(单位:毫秒)
     */

    public static int serverRequestTimeOut;


    /**
     * 向服务端连接超时时间设置(单位:毫秒)
     */

    public static int serverConnectTimeOut;

    /**
     * 服务端响应超时时间设置(单位:毫秒)
     */

    public static int serverResponseTimeOut;



    @Value("${http.pool.max.connection}")
    public void setMaxConnectionNum(int mcn) {
        maxConnectionNum = mcn;
    }

    @Value("${http.pool.max.per.route}")
    public void setMaxPerRoute(int mpr) {
        maxPerRoute = mpr;
    }

    @Value("${http.pool.request.timeout}")
    public void setServerRequestTimeOut(int sreq) {
        serverRequestTimeOut = sreq;
    }

    @Value("${http.pool.connect.timeout}")
    public void setServerConnectTimeOut(int srco) {serverConnectTimeOut = srco;}



    @Value("${http.pool.response.timeout}")
    public void setServerResponseTimeOut(int sres) {
        serverResponseTimeOut = sres;
    }




}
