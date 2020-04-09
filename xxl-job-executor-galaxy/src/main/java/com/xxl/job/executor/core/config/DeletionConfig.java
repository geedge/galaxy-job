package com.xxl.job.executor.core.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class DeletionConfig {
	
	public static String trafficServer;
	public static String trafficDataSource;
	public static String trafficUserName;
	public static String trafficPassword;
	public static String trafficTableNames;
	public static String clusterName = "ck_cluster";
	
	public static String analyticServer;
	public static String selectAnalyticServer;
	
	
	public String getTrafficServer() {
		return trafficServer;
	}
	@Value("${delete.traffic.server}")
	public void setTrafficServer(String trafficServer) {
		DeletionConfig.trafficServer = trafficServer;
	}
	public String getTrafficDataSource() {
		return trafficDataSource;
	}
	@Value("${delete.traffic.datasource}")
	public void setTrafficDataSource(String trafficDataSource) {
		DeletionConfig.trafficDataSource = trafficDataSource;
	}
	public String getTrafficUserName() {
		return trafficUserName;
	}
	@Value("${delete.traffic.username}")
	public void setTrafficUserName(String trafficUserName) {
		DeletionConfig.trafficUserName = trafficUserName;
	}
	public String getTrafficPassword() {
		return trafficPassword;
	}
	@Value("${delete.traffic.password}")
	public void setTrafficPassword(String trafficPassword) {
		DeletionConfig.trafficPassword = trafficPassword;
	}
	public String getTrafficTableNames() {
		return trafficTableNames;
	}
	@Value("${delete.traffic.tablenames}")
	public void setTrafficTableNames(String trafficTableNames) {
		DeletionConfig.trafficTableNames = trafficTableNames;
	}
	public String getAnalyticServer() {
		return analyticServer;
	}
	@Value("${delete.analytic.server}")
	public void setAnalyticServer(String analyticServer) {
		DeletionConfig.analyticServer = analyticServer;
	}
	public String getSelectAnalyticServer() {
		return selectAnalyticServer;
	}
	@Value("${select.analytic.server}")
	public void setSelectAnalyticServer(String selectAnalyticServer) {
		DeletionConfig.selectAnalyticServer = selectAnalyticServer;
	}
	
	public static String getClusterName() {
		return clusterName;
	}
	public static void setClusterName(String clusterName) {
		DeletionConfig.clusterName = clusterName;
	}
	@Override
	public String toString() {
		return "DeletionConfig [getTrafficServer()=" + getTrafficServer() + ", getTrafficDataSource()="
				+ getTrafficDataSource() + ", getTrafficUserName()=" + getTrafficUserName() + ", getTrafficPassword()="
				+ getTrafficPassword() + ", getTrafficTableNames()=" + getTrafficTableNames() + ", getAnalyticServer()="
				+ getAnalyticServer() + ", getSelectAnalyticServer()=" + getSelectAnalyticServer() + "]";
	}
	

}
