package com.xxl.job.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * @Date: 2020-11-12 12:02
 * @Author : liuyongqiang
 * @ClassName : MyX509TrustManager
 * @Description : 自定义X509TrustManager证书管理器
 */
public class MyX509TrustManager implements X509TrustManager {

    private static Logger logger = LoggerFactory.getLogger(MyX509TrustManager.class);

    X509TrustManager pkixTrustManager;

    MyX509TrustManager() throws Exception {
        // create a "default" JSSE X509TrustManager.

        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(new FileInputStream("trustedCerts"), "passphrase".toCharArray());

        TrustManagerFactory tmf = TrustManagerFactory.getInstance("PKIX");
        tmf.init(ks);

        TrustManager tms[] = tmf.getTrustManagers();

        /*
         * 遍历返回的信任管理器,找到X509TrustManager 的管理器作为默认的信任管理器
         */
        for (int i = 0; i < tms.length; i++) {
            if (tms[i] instanceof X509TrustManager) {
                pkixTrustManager = (X509TrustManager) tms[i];
                return;
            }
        }

        /*
         *没有找到,初始化信任管理器失败
         */
        throw new Exception("Couldn't initialize");
    }

    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        try {
            pkixTrustManager.checkClientTrusted(chain, authType);
        } catch (CertificateException excep) {
            logger.error("checkClientTrusted:{}", excep);
        }

    }

    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        try {
            pkixTrustManager.checkServerTrusted(chain, authType);
        } catch (CertificateException excep) {
            logger.error("checkServerTrusted:{}", excep);
        }

    }

    public X509Certificate[] getAcceptedIssuers() {
        return pkixTrustManager.getAcceptedIssuers();
    }
}
