package com.mesalab.executor.core.utils;

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
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HttpClientUtils {
    /** 全局连接池对象 */
    private static final PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();

    private static Logger logger = LoggerFactory.getLogger(HttpClientUtils.class);

    /**
     * 静态代码块配置连接池信息
     */
    static {

        // 设置最大连接数
        connManager.setMaxTotal(HttpConfig.maxConnectionNum);
        // 设置每个连接的路由数
        connManager.setDefaultMaxPerRoute(HttpConfig.maxPerRoute);

    }

    /**
     * 获取Http客户端连接对象
     * @return Http客户端连接对象
     */
    public static CloseableHttpClient getHttpClient() {
        // 创建Http请求配置参数
        RequestConfig requestConfig = RequestConfig.custom()
                // 获取连接超时时间
                .setConnectionRequestTimeout(HttpConfig.serverRequestTimeOut)
                // 请求超时时间
                .setConnectTimeout(HttpConfig.serverConnectTimeOut)
                // 响应超时时间
                .setSocketTimeout(HttpConfig.serverResponseTimeOut)
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
                if (exception instanceof InterruptedIOException) {// 超时
                    return true;
                }
                if (exception instanceof UnknownHostException) {// 目标服务器不可达
                    return false;
                }
                if (exception instanceof ConnectTimeoutException) {// 连接被拒绝
                    return false;
                }
                if (exception instanceof SSLException) {// ssl握手异常
                    return false;
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
                .setConnectionManager(connManager)
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
            e.printStackTrace();
        } catch (ClientProtocolException e) {
            logger.error("协议错误: {}", e.getMessage());
            e.printStackTrace();

        } catch (ParseException e) {
            logger.error("解析错误: {}", e.getMessage());
            e.printStackTrace();

        } catch (IOException e) {
            logger.error("IO错误: {}",e.getMessage());
            e.printStackTrace();

        }  finally {
            if (null != response) {
                try {
                    EntityUtils.consume(response.getEntity());
                    response.close();
                } catch (IOException e) {
                    logger.error("释放链接错误: {}", e.getMessage());
                    e.printStackTrace();
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
            	httpPost.setEntity(new ByteArrayEntity(requestBody.getBytes("utf-8")));
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
            e.printStackTrace();

		} catch (ClientProtocolException e) {
            logger.error("协议错误: {}", e.getMessage());
            e.printStackTrace();

        } catch (ParseException e) {
        	logger.error("解析错误: {}", e.getMessage());
            e.printStackTrace();

        } catch (IOException e) {
        	logger.error("IO错误: {}", e.getMessage());
            e.printStackTrace();

        } finally {
            if (null != response) {
                try {
                    EntityUtils.consumeQuietly(response.getEntity());
                    response.close();
                } catch (IOException e) {
                	logger.error("释放链接错误: {}", e.getMessage());
                    e.printStackTrace();
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
        
        // 创建POST请求对象
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
            e.printStackTrace();

		} catch (ClientProtocolException e) {
            logger.error("协议错误: {}", e.getMessage());
            e.printStackTrace();

        } catch (ParseException e) {
        	logger.error("解析错误: {}", e.getMessage());
            e.printStackTrace();

        } catch (IOException e) {
        	logger.error("IO错误: {}", e.getMessage());
            e.printStackTrace();

        } finally {
            if (null != response) {
                try {
                    EntityUtils.consumeQuietly(response.getEntity());
                    response.close();
                } catch (IOException e) {
                	logger.error("释放链接错误: {}", e.getMessage());
                    e.printStackTrace();
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
            
        } catch (Exception e) {    
            e.printStackTrace();    
        }finally{    
        	 if (null != response) {
                 try {
                     EntityUtils.consumeQuietly(response.getEntity());
                     response.close();
                 } catch (IOException e) {
                 	logger.error("释放链接错误: {}", e.getMessage());
                     e.printStackTrace();
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
            e.printStackTrace();
        } catch (ParseException e) {
            logger.error("current file: {}, Parser error:{}", url, e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            logger.error("current file: {},IO error:{}", url, e.getMessage());
            e.printStackTrace();
        } finally {
            if (null != response) {
                try {
                    EntityUtils.consume(response.getEntity());
                    response.close();
                } catch (IOException e) {
                    logger.error("Release Connection error:{}", e.getMessage());
                    e.printStackTrace();
                }
            }
            return result;
        }
    }



    public static class IdleConnectionMonitorThread extends Thread {

        private final HttpClientConnectionManager connMgr;
        private volatile boolean shutdown;

        public IdleConnectionMonitorThread(HttpClientConnectionManager connMgr) {
            super();
            this.connMgr = connMgr;
        }

        @Override
        public void run() {
            try {
                while (!shutdown) {
                    synchronized (this) {
                        wait(5000);
                        // Close expired connections
                        connMgr.closeExpiredConnections();
                        // Optionally, close connections
                        // that have been idle longer than 30 sec
                        connMgr.closeIdleConnections(30, TimeUnit.SECONDS);
                    }
                }
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }

        public void shutdown() {
            shutdown = true;
            synchronized (this) {
                notifyAll();
            }
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
        } catch (Exception e) {
            throw e;
        }
    }



}
