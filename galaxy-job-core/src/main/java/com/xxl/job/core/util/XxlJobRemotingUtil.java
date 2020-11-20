package com.xxl.job.core.util;

import com.xxl.job.core.biz.model.ReturnT;
import io.netty.util.internal.StringUtil;
import org.apache.commons.io.input.BoundedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.util.Arrays;

/**
 * @author xuxueli 2018-11-25 00:55:31
 */
public class XxlJobRemotingUtil {
    private static Logger logger = LoggerFactory.getLogger(XxlJobRemotingUtil.class);
    public static final String XXL_JOB_ACCESS_TOKEN = "XXL-JOB-ACCESS-TOKEN";

    private static String[] VERIFY_HOST_NAME_ARRAY = new String[]{};

    // trust-https start
    private static void trustAllHosts(HttpsURLConnection connection) {
        try {
            SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(null, null, new java.security.SecureRandom());
            SSLSocketFactory newFactory = sc.getSocketFactory();
            if (newFactory != null) {
                connection.setSSLSocketFactory(newFactory);
            }
        } catch (Exception e) {
            logger.error("errorMessage:{}", e);
        }
        connection.setHostnameVerifier((hostname, session) -> {
            if (StringUtil.isNullOrEmpty(hostname)) {
                return false;
            }
            return !Arrays.asList(VERIFY_HOST_NAME_ARRAY).contains(hostname);
        });
    }

   /* private static final TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
            return new java.security.cert.X509Certificate[]{};
        }

        public void checkClientTrusted(X509Certificate[] chain, String authType) {
        }

        public void checkServerTrusted(X509Certificate[] chain, String authType) {
        }
    }};*/
    // trust-https end


    /**
     * post
     *
     * @param accessToken
     * @param timeout
     * @param requestObj
     * @param returnTargClassOfT
     * @return
     */
    public static ReturnT postBody(String ul, String accessToken, int timeout, Object requestObj, Class returnTargClassOfT) {
        HttpURLConnection connection = null;
        BufferedReader bufferedReader = null;
        BoundedInputStream boundedInput = null;
        String xxljobaddr = String.format("for ,%s", " :" + ul);
        try {
            // connection
//            URL realul = new URL(ul);
//            connection = (HttpURLConnection) realul.openConnection();

            // trust-https
            boolean useHttps = ul.startsWith("https");
            if (useHttps) {
                HttpsURLConnection https = (HttpsURLConnection) connection;
                trustAllHosts(https);
            }

            // connection setting
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setUseCaches(false);
            connection.setReadTimeout(timeout * 10000);
            connection.setConnectTimeout(3 * 10000);
            connection.setRequestProperty("connection", "Keep-Alive");
            connection.setRequestProperty("Content-Type", "application/json;charset=UTF-8");
            connection.setRequestProperty("Accept-Charset", "application/json;charset=UTF-8");

            if (accessToken != null && accessToken.trim().length() > 0) {
                connection.setRequestProperty(XXL_JOB_ACCESS_TOKEN, accessToken);
            }

            // do connection
            connection.connect();

            // write requestBody
            if (requestObj != null) {
                String requestBody = GsonTool.toJson(requestObj);

                try (DataOutputStream dataOutputStream = new DataOutputStream(connection.getOutputStream())) {
                    dataOutputStream.write(requestBody.getBytes("UTF-8"));
                    dataOutputStream.flush();
                }
            }

            /*byte[] requestBodyBytes = requestBody.getBytes("UTF-8");
            connection.setRequestProperty("Content-Length", String.valueOf(requestBodyBytes.length));
            OutputStream outwritestream = connection.getOutputStream();
            outwritestream.write(requestBodyBytes);
            outwritestream.flush();
            outwritestream.close();*/

            // valid StatusCode
//            int statusCode = connection.getResponseCode();
//            if (statusCode != 200) {
//                return new ReturnT<String>(ReturnT.FAIL_CODE, "xxl-rpc remoting fail, StatusCode(" + statusCode + ") invalid. for  : " + xxljobaddr);
//            }

            boundedInput = new BoundedInputStream(connection.getInputStream(), 30000 * 1024);//限制最大读取字节数
            // result
            bufferedReader = new BufferedReader(new InputStreamReader(boundedInput), 10 * 1024);
            StringBuilder result = new StringBuilder();
            String line;
            /*while ((line = bufferedReader.readLine()) != null) {
                result.append(line);
            }*/
            final int MAX_STR_LEN = 100_000_000;
            int intC;
            while ((intC = bufferedReader.read()) != -1) {
                char c = (char) intC;
                if (c == '\n') {
                    break;
                }
                if (result.length() >= MAX_STR_LEN) {
                    throw new RuntimeException("input too long");
                }
                result.append(c);
            }

            String resultJson = result.toString();

            // parse returnT
            try {
                ReturnT returnT = GsonTool.fromJson(resultJson, ReturnT.class, returnTargClassOfT);
                return returnT;
            } catch (Exception e) {
//                logger.error("xxl-rpc remoting (url=" + xxljobaddr + ") response content invalid(" + resultJson + ").", e);
                logger.error("errorMessage:{}", e);
                return new ReturnT<String>(ReturnT.FAIL_CODE, "xxl-rpc remoting (url=" + xxljobaddr + ") response content invalid.");
            }

        } catch (Exception e) {
            logger.error("errorMessage:{}", e);
            return new ReturnT<String>(ReturnT.FAIL_CODE, "xxl-rpc remoting error, for url : " + xxljobaddr);
        } finally {
            try {
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
                if (boundedInput != null) {
                    boundedInput.close();
                }
                if (connection != null) {
                    connection.disconnect();
                }
            } catch (Exception e2) {
                logger.error("errorMessage:{}", e2);
            }
        }
    }

    public static boolean isEqual(String astr, String bstr) {
        if (astr == null || bstr == null) {
            return false;
        }
        byte[] a = astr.getBytes();
        byte[] b = bstr.getBytes();
        if (a.length != b.length) {
            return false;
        }

        int result = 0;
        for (int i = 0; i < a.length; i++) {
            result |= a[i] ^ b[i];
        }
        return result == 0;
    }
}
