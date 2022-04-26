package com.mesalab.executor.core.utils.async;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.concurrent.ExecutionException;


public class HttpKit {

    private static final String USER_AGENT = "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; .NET CLR 2.0.50727";
    private static final int CONNECT_TIME_OUT = 3000; //链接超时时间
    private static final int REQUEST_TIME_OUT = 3000; //请求超时时间
    private static final int READ_TIME_OUT = 5000; //请求超时时间
    private static final int MAX_CONNECTIONS = 100000; //最大连接数

    private static SSLContext ssl_context = null;

    static {
        // 在调用SSL之前需要重写验证方法，取消检测SSL
        X509TrustManager trustManager = new X509TrustManager() {
            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return null;
            }
            @Override
            public void checkClientTrusted(X509Certificate[] xcs, String str) {
            }
            @Override
            public void checkServerTrusted(X509Certificate[] xcs, String str) {
            }
        };
        try {
            ssl_context = SSLContext.getInstance(SSLConnectionSocketFactory.TLS);
            ssl_context.init(null, new TrustManager[]{trustManager}, null);
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            e.printStackTrace();
        }
    }

    public static AsyncHttpClient getHttpClient() {
        AsyncHttpClient http = new AsyncHttpClient(new AsyncHttpClientConfig.Builder()
                .setMaxConnections(MAX_CONNECTIONS)
                .setSSLContext(ssl_context)
                .setRequestTimeout(REQUEST_TIME_OUT)
                .setMaxRequestRetry(1)
                .setReadTimeout(READ_TIME_OUT)
                .setFollowRedirect(true)
                .setMaxRedirects(3)
                .setConnectTimeout(CONNECT_TIME_OUT)
                .build());
        return http;
    }

    /**
     * @param url 请求地址
     * @return 请求失败返回null
     * @description 功能描述: get 请求
     */
    public static int getStatus(String url) {
        AsyncHttpClient httpClient = getHttpClient();
        AsyncHttpClient.BoundRequestBuilder builder = httpClient.prepareGet(url);
        builder.addHeader("User-Agent", USER_AGENT);
        int statusCode = 0;
        try {
            statusCode = builder.execute().get().getStatusCode();
        } catch (InterruptedException e) {
            return statusCode;
        } catch (ExecutionException e) {
            return statusCode;
        } finally {
            httpClient.close();
        }
        return statusCode;
    }


}