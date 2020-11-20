package com.mesalab.executor.core.utils;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class Constant {

	// 存储配额中区分数据类型的字段
	public static final String FILES ="Files";
	public static final String REPORT_AND_METRICS ="Report and Metrics";
	public static final String TRAFFIC_LOGS ="Traffic Logs";
	
	// zookeeper /path+node
	public static final String ZK_TRAFFIC_LOGS = "Traffic-Logs";
	public static final String ZK_REPORT_AND_METRICS = "Report-and-Metrics";


	public static String SERVER_PROTOCOL = "https://";



	@Value("${server.protocol}")
	public void setServerProtocol(String serverProtocol) {
		SERVER_PROTOCOL = serverProtocol;
	}
}
