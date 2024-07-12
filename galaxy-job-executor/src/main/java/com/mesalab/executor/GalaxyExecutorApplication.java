package com.mesalab.executor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.TimeZone;


@SpringBootApplication
public class GalaxyExecutorApplication {

	public static void main(String[] args) {
		System.setProperty("JM.SNAPSHOT.PATH", "config");
		System.setProperty("user.timezone", "UTC");
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		System.setProperty("jasypt.encryptor.password", "galaxy");
        SpringApplication.run(GalaxyExecutorApplication.class, args);
	}
}