package com.mesalab.executor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.TimeZone;


@SpringBootApplication
//@SpringBootApplication(exclude= {DataSourceAutoConfiguration.class})
public class GalaxyExecutorApplication {

	public static void main(String[] args) {
		System.setProperty("user.timezone", "UTC");
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		System.setProperty("jasypt.encryptor.password", "galaxy");
        SpringApplication.run(GalaxyExecutorApplication.class, args);
	}

}