package com.xxl.job.admin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author xuxueli 2018-10-28 00:38:13
 */
@SpringBootApplication
public class XxlJobAdminApplication {

    public static void main(String[] args) {
        /** 配置加解密秘钥，与配置文件的密文分开放 */
        System.setProperty("jasypt.encryptor.password", "galaxy@job!2020");
        SpringApplication.run(XxlJobAdminApplication.class, args);
    }

}