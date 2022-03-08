package com.mesalab.executor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class GalaxyExecutorApplication {

	public static void main(String[] args) {
		System.setProperty("user.timezone", "UTC");
		System.setProperty("jasypt.encryptor.password", "galaxy");
        SpringApplication.run(GalaxyExecutorApplication.class, args);
	}

}