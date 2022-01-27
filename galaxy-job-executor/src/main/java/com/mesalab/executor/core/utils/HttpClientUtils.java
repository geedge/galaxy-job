package com.mesalab.executor.core.utils;

import cn.hutool.log.Log;
import com.mesalab.executor.core.config.HttpConfig;
import com.mesalab.executor.exception.BusinessException;
import com.zdjizhi.utils.StringUtil;
import org.apache.commons.io.IOUtils;
import org.apache.http.*;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.Map;

public class HttpClientUtils {

    private static Log logger = Log.get();

    private static HttpConfig httpConfig = (HttpConfig) SpringContextUtil.getBean("httpConfig");
    /**
     * 在调用SSL之前需要重写验证方法，取消检测SSL
     * 创建ConnectionManager，添加Connection配置信息
     *
     * @return HttpClient 支持https
     */
    private static PoolingHttpClientConnectionManager getSslClientManager() {
        try {
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
            SSLContext ctx = SSLContext.getInstance(SSLConnectionSocketFactory.TLS);
            ctx.init(null, new TrustManager[]{trustManager}, null);
            SSLConnectionSocketFactory socketFactory = new SSLConnectionSocketFactory(ctx, NoopHostnameVerifier.INSTANCE);
            Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("http", PlainConnectionSocketFactory.INSTANCE)
                    .register("https", socketFactory).build();
            // 创建ConnectionManager，添加Connection配置信息
            PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
            // 设置最大连接数
            connManager.setMaxTotal(httpConfig.getMaxConnectionNum());
            // 设置每个连接的路由数
            connManager.setDefaultMaxPerRoute(httpConfig.getMaxPerRoute());
            return connManager;
        } catch (KeyManagementException e) {
            logger.error(e.getMessage());
            throw new BusinessException(e);
        } catch (NoSuchAlgorithmException e) {
            logger.error(e.getMessage());
            throw new BusinessException(e);
        }
    }

    /**
     * 获取Http客户端连接对象
     * @return Http客户端连接对象
     */
    public static CloseableHttpClient getHttpClient() {
        // 创建Http请求配置参数
        RequestConfig requestConfig = RequestConfig.custom()
                // 获取连接超时时间
                .setConnectionRequestTimeout(httpConfig.getServerRequestTimeOut())
                // 请求超时时间
                .setConnectTimeout(httpConfig.getServerConnectTimeOut())
                // 响应超时时间
                .setSocketTimeout(httpConfig.getServerResponseTimeOut())
                .build();

        /**
         * 测出超时重试机制为了防止超时不生效而设置
         *  如果直接放回false,不重试
         *  这里会根据情况进行判断是否重试
         */
        HttpRequestRetryHandler retry = new HttpRequestRetryHandler() {
            @Override
            public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
                if (executionCount >= 3) {// 如果已经重试了3次，就放弃
                    return false;
                }
                if (exception instanceof NoHttpResponseException) {// 如果服务器丢掉了连接，那么就重试
                    return true;
                }
                if (exception instanceof SSLHandshakeException) {// 不要重试SSL握手异常
                    return false;
                }
                if (exception instanceof UnknownHostException) {// 目标服务器不可达
                    return false;
                }
                if (exception instanceof ConnectTimeoutException) {// 连接被拒绝
                    return false;
                }
                if (exception instanceof HttpHostConnectException) {// 连接被拒绝
                    return false;
                }
                if (exception instanceof SSLException) {// ssl握手异常
                    return false;
                }
                if (exception instanceof InterruptedIOException) {// 超时
                    return true;
                }
                HttpClientContext clientContext = HttpClientContext.adapt(context);
                HttpRequest request = clientContext.getRequest();
                // 如果请求是幂等的，就再次尝试
                if (!(request instanceof HttpEntityEnclosingRequest)) {
                    return true;
                }
                return false;
            }
        };


        ConnectionKeepAliveStrategy myStrategy = new ConnectionKeepAliveStrategy() {
            @Override
            public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
                HeaderElementIterator it = new BasicHeaderElementIterator
                        (response.headerIterator(HTTP.CONN_KEEP_ALIVE));
                while (it.hasNext()) {
                    HeaderElement he = it.nextElement();
                    String param = he.getName();
                    String value = he.getValue();
                    if (value != null && param.equalsIgnoreCase
                            ("timeout")) {
                        return Long.parseLong(value) * 1000;
                    }
                }
                return 60 * 1000;//如果没有约定，则默认定义时长为60s
            }
        };

        // 创建httpClient
        return HttpClients.custom()
                // 把请求相关的超时信息设置到连接客户端
                .setDefaultRequestConfig(requestConfig)
                // 把请求重试设置到连接客户端
                .setRetryHandler(retry)
                .setKeepAliveStrategy(myStrategy)
                // 配置连接池管理对象
                .setConnectionManager(getSslClientManager())
                .build();
    }


    /**
     * GET请求
     *
     * @param url 请求地
     * @return
     */
    public static String httpGet(String url, Header... headers) {
        String msg = "-1";

        // 获取客户端连接对象
        CloseableHttpClient httpClient = getHttpClient();
        CloseableHttpResponse response = null;

        try {

            URL ul = new URL(url);

            URI uri = new URI(ul.getProtocol(),null,  ul.getHost(),  ul.getPort(), ul.getPath(), ul.getQuery(), null);
            logger.info("http get uri {}",uri);
            // 创建GET请求对象
            HttpGet httpGet = new HttpGet(uri);
            
            if (StringUtil.isNotEmpty(headers)) {
                for (Header h : headers) {
                	httpGet.addHeader(h);
                    logger.info("request header : {}",h);
                }
            }
            // 执行请求
            response = httpClient.execute(httpGet);
            int statusCode = response.getStatusLine().getStatusCode();
            // 获取响应实体
            HttpEntity entity = response.getEntity();
            // 获取响应信息
            msg = EntityUtils.toString(entity, "UTF-8");

            if (statusCode != HttpStatus.SC_OK) {
                throw new BusinessException(response.getStatusLine().getStatusCode(),
                        "Http get content is :" + msg);
            }

        } catch (URISyntaxException e) {
            logger.error("URI 转换错误: {}", e.getMessage());
        } catch (ClientProtocolException e) {
            logger.error("协议错误: {}", e.getMessage());
        } catch (ParseException e) {
            logger.error("解析错误: {}", e.getMessage());
        } catch (IOException e) {
            logger.error("IO错误: {}",e.getMessage());
        }  finally {
            if (null != response) {
                try {
                    EntityUtils.consume(response.getEntity());
                    response.close();
                } catch (IOException e) {
                    logger.error("释放链接错误: {}", e.getMessage());

                }
            }
        }

        return msg;
    }
    /**
     * POST 请求
     * @param url
     * @param requestBody
     * @return
     */
    public static String httpPost(String url, String requestBody, Header... headers) {
    	String msg = "-1";
    	// 获取客户端连接对象
        CloseableHttpClient httpClient = getHttpClient();
        
        // 创建POST请求对象
        CloseableHttpResponse response = null;
        try {
        	
        	URL ul = new URL(url);

            URI uri = new URI(ul.getProtocol(),null,  ul.getHost(),  ul.getPort(), ul.getPath(), ul.getQuery(), null);

            logger.info("http post uri:{}， http post body:{}", uri, requestBody);
            
            HttpPost httpPost = new HttpPost(uri);
            httpPost.setHeader("Content-Type", "application/json");
            if (StringUtil.isNotEmpty(headers)) {
                for (Header h : headers) {
                    httpPost.addHeader(h);
                    logger.info("request header : {}",h);
                }
            }

            if(StringUtil.isNotBlank(requestBody)) {
                if(requestBody!=null){
                    byte[] bytes = requestBody.getBytes(StandardCharsets.UTF_8);
                    httpPost.setEntity(new ByteArrayEntity(bytes));
                }
            }
            
            response = httpClient.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            // 获取响应实体
            HttpEntity entity = response.getEntity();
            // 获取响应信息
            msg = EntityUtils.toString(entity, "UTF-8");
            
            if (statusCode != HttpStatus.SC_OK) {
                throw new BusinessException(response.getStatusLine().getStatusCode(),
                        "Http post content is :" + msg);
            }
        } catch (URISyntaxException e) {
        	logger.error("URI 转换错误: {}", e.getMessage());
		} catch (ClientProtocolException e) {
            logger.error("协议错误: {}", e.getMessage());
        } catch (ParseException e) {
        	logger.error("解析错误: {}", e.getMessage());
        } catch (IOException e) {
        	logger.error("IO错误: {}", e.getMessage());
        } finally {
            if (null != response) {
                try {
                    EntityUtils.consumeQuietly(response.getEntity());
                    response.close();
                } catch (IOException e) {
                	logger.error("释放链接错误: {}", e.getMessage());

                }
            }
        }
        return msg;
    }
    /**
     * PUT 请求
     * @param url
     * @param requestBody
     * @return
     */
    public static String httpPut(String url, String requestBody, Header... headers) {
    	String msg = "-1";
    	// 获取客户端连接对象
        CloseableHttpClient httpClient = getHttpClient();
        CloseableHttpResponse response = null;
        try {
        	
        	URL ul = new URL(url);

            URI uri = new URI(ul.getProtocol(),null,  ul.getHost(),  ul.getPort(), ul.getPath(), ul.getQuery(), null);

            logger.info("http post uri:{}， http post body:{}", uri, requestBody);
            
            HttpPut httpPut = new HttpPut(uri);
            httpPut.setHeader("Content-Type", "application/json");
            if (StringUtil.isNotEmpty(headers)) {
                for (Header h : headers) {
                	httpPut.addHeader(h);
                    logger.info("request header : {}",h);
                }
            }

            if(StringUtil.isNotBlank(requestBody)) {
            	httpPut.setEntity(new ByteArrayEntity(requestBody.getBytes("utf-8")));
            }
            
            response = httpClient.execute(httpPut);
            int statusCode = response.getStatusLine().getStatusCode();
            // 获取响应实体
            HttpEntity entity = response.getEntity();
            // 获取响应信息
            msg = EntityUtils.toString(entity, "UTF-8");
            
            if (statusCode != HttpStatus.SC_OK) {
                throw new BusinessException(response.getStatusLine().getStatusCode(),
                        "Http put content is :" + msg);
            }
        } catch (URISyntaxException e) {
        	logger.error("URI 转换错误: {}", e.getMessage());
		} catch (ClientProtocolException e) {
            logger.error("协议错误: {}", e.getMessage());
        } catch (ParseException e) {
        	logger.error("解析错误: {}", e.getMessage());
        } catch (IOException e) {
        	logger.error("IO错误: {}", e.getMessage());
        } finally {
            if (null != response) {
                try {
                    EntityUtils.consumeQuietly(response.getEntity());
                    response.close();
                } catch (IOException e) {
                	logger.error("释放链接错误: {}", e.getMessage());

                }
            }
        }
        return msg;
    }
    /**
     * PUT 请求
     * @param url
     * @param headers
     * @return
     */
    public static String httpPutStream(String url, InputStream in, Header... headers) {
        String msg = "-1";
        // 获取客户端连接对象
        CloseableHttpClient httpClient = getHttpClient();
        CloseableHttpResponse response = null;
        try {

            URL ul = new URL(url);
            URI uri = new URI(ul.getProtocol(),null,  ul.getHost(),  ul.getPort(), ul.getPath(), ul.getQuery(), null);

            logger.info("http post uri:{} ", url);

            HttpPut httpPut = new HttpPut(uri);
            httpPut.setHeader("Content-Type", "application/zip");
            if (StringUtil.isNotEmpty(headers)) {
                for (Header h : headers) {
                    httpPut.addHeader(h);
                    logger.info("request header : {}",h);
                }
            }

            httpPut.setEntity(new InputStreamEntity(in));

            response = httpClient.execute(httpPut);
            int statusCode = response.getStatusLine().getStatusCode();
            // 获取响应实体
            HttpEntity entity = response.getEntity();
            // 获取响应信息
            msg = EntityUtils.toString(entity, "UTF-8");

            if (statusCode != HttpStatus.SC_OK) {
                throw new BusinessException(response.getStatusLine().getStatusCode(),
                        "Http put content is :" + msg);
            }
        } catch (URISyntaxException e) {
            logger.error("URI 转换错误: {}", e.getMessage());
        } catch (ClientProtocolException e) {
            logger.error("协议错误: {}", e.getMessage());
        } catch (ParseException e) {
            logger.error("解析错误: {}", e.getMessage());
        } catch (IOException e) {
            logger.error("IO错误: {}", e.getMessage());
        } finally {
            if (null != response) {
                try {
                    EntityUtils.consumeQuietly(response.getEntity());
                    response.close();
                } catch (IOException e) {
                    logger.error("释放链接错误: {}", e.getMessage());

                }
            }
        }
        return msg;
    }
    /**  
     * 发送http delete请求  
     */    
    public static String httpDelete(String url, Header... headers){
    	String msg = "-1";
    	// 获取客户端连接对象
        CloseableHttpClient httpClient = getHttpClient();
        
        // 创建POST请求对象
        CloseableHttpResponse response = null;
    	
        try {    
        	URL ul = new URL(url);

            URI uri = new URI(ul.getProtocol(),null,  ul.getHost(),  ul.getPort(), ul.getPath(), ul.getQuery(), null);

            logger.info("http delete uri:{}", uri);
            
            HttpDelete httpdelete = new HttpDelete(uri);
            if (StringUtil.isNotEmpty(headers)) {
                for (Header h : headers) {
                	httpdelete.addHeader(h);
                    logger.info("request header : {}",h);
                }
            }
            response = httpClient.execute(httpdelete);
            int statusCode = response.getStatusLine().getStatusCode();
            // 获取响应实体
            HttpEntity entity = response.getEntity();
            // 获取响应信息
            msg = EntityUtils.toString(entity, "UTF-8");
            
            if (statusCode != HttpStatus.SC_OK) {
                throw new BusinessException(response.getStatusLine().getStatusCode(),
                        "Http post content is :" + msg);
            }
            
        } catch (URISyntaxException e) {
            logger.error("URI 转换错误: {}", e.getMessage());
        } catch (ClientProtocolException e) {
            logger.error("协议错误: {}", e.getMessage());
        } catch (ParseException e) {
            logger.error("解析错误: {}", e.getMessage());

        } catch (IOException e) {
            logger.error("IO错误: {}", e.getMessage());

        }finally{
        	 if (null != response) {
                 try {
                     EntityUtils.consumeQuietly(response.getEntity());
                     response.close();
                 } catch (IOException e) {
                 	logger.error("释放链接错误: {}", e.getMessage());

                 }
             }
         } 
        return msg;    
    }    



    public static InputStream httpGetInputStream(String url) {
        InputStream result = null;
        // 获取客户端连接对象
        CloseableHttpClient httpClient = getHttpClient();
        // 创建GET请求对象
        HttpGet httpGet = new HttpGet(url);

        CloseableHttpResponse response = null;

        try {
            // 执行请求
            response = httpClient.execute(httpGet);
            // 获取响应实体
            result = IOUtils.toBufferedInputStream(response.getEntity().getContent());
            // 获取响应信息
            EntityUtils.consume(response.getEntity());
        } catch (ClientProtocolException e) {
            logger.error("current file: {},Protocol error:{}", url, e.getMessage());

        } catch (ParseException e) {
            logger.error("current file: {}, Parser error:{}", url, e.getMessage());

        } catch (IOException e) {
            logger.error("current file: {},IO error:{}", url, e.getMessage());

        } finally {
            if (null != response) {
                try {
                    EntityUtils.consume(response.getEntity());
                    response.close();
                } catch (IOException e) {
                    logger.error("Release Connection error:{}", e.getMessage());

                }
            }
            return result;
        }
    }

    /**
     * 拼装url
     * url ,参数map
     */
    public static String getUrlWithParams(String url, Map<String, Object> params) {
        try {
            StringBuffer query = new StringBuffer();
            for (Map.Entry<String, Object> kv : params.entrySet()) {
                query.append(kv.getKey() + "=");
                query.append(kv.getValue() + "&");
            }
            if (query.lastIndexOf("&") > 0) {
                query.deleteCharAt(query.length() - 1);
            }
            return url + "?" + query.toString();
        } catch (BusinessException e) {
            throw e;
        }
    }



}
