package com.xxl.job.admin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.TimeZone;

/**
 * @author xuxueli 2018-10-28 00:38:13
 */
@SpringBootApplication
public class XxlJobAdminApplication {

	public static void main(String[] args) {
		System.setProperty("JM.SNAPSHOT.PATH", "config");
		System.setProperty("user.timezone", "UTC");
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		System.setProperty("jasypt.encryptor.password", "galaxy");
        SpringApplication.run(XxlJobAdminApplication.class, args);

	}

}