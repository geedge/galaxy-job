package com.xxl.job.executor.core.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class StorgeConfig {

	public static String storgeTrafficServer;// clickhouse要查看的ip
	
	public static String storgeTrafficUsername;// clickhouse要查看的username
	
	public static String storgeTrafficPassword;// clickhouse要查看的password
	
	public static String storgeAnalyticServer;// druid 查询节点ip和port
	
	public static String storgeFilesServer;// minio ip
	
	/**
	 * 获取修改存储配额脚本参数命令
	 * @param scriptDirectory 脚本所在文件绝对路径
	 * DEPLOY=yes 标识脚本的第一次部署已经没了
	 */
	public static String getStorgeConfigCommand (String scriptDirectory) {
		StringBuffer sb = new StringBuffer();
		sb.append("sed -i \"s%DEPLOY=no%DEPLOY=yes%g\" "+scriptDirectory+"*.sh;");
		sb.append("sed -i \"s%BASE_DIR=.*%BASE_DIR="+scriptDirectory+"%g\" "+scriptDirectory+"*.sh;");
		sb.append("sed -i \"s%TRAFFIC_SERVER=.*%TRAFFIC_SERVER="+StorgeConfig.storgeTrafficServer+"%g\" "+scriptDirectory+"*.sh;");
		sb.append("sed -i \"s%TRAFFIC_USERNAME=.*%TRAFFIC_USERNAME="+StorgeConfig.storgeTrafficUsername+"%g\" "+scriptDirectory+"*.sh;");
		sb.append("sed -i \"s%TRAFFIC_PASSWORD=.*%TRAFFIC_PASSWORD="+StorgeConfig.storgeTrafficPassword+"%g\" "+scriptDirectory+"*.sh;");
		sb.append("sed -i \"s%ANALYTIC_SERVER=.*%ANALYTIC_SERVER="+StorgeConfig.storgeAnalyticServer+"%g\" "+scriptDirectory+"*.sh;");
		sb.append("sed -i \"s%FILES_SERVER=.*%FILES_SERVER="+StorgeConfig.storgeFilesServer+"%g\" "+scriptDirectory+"*.sh;");
		sb.append("chmod -R +x "+scriptDirectory+";");
		return sb.toString();
	}
	/**
	 * 判断是否已经部署过脚步，并修改了参数
	 * @return
	 */
	public static String getDeployScriptCommand (String scriptDirectory) {
		StringBuffer sb = new StringBuffer();
		sb.append("cat "+scriptDirectory+"*.sh|grep DEPLOY=yes||echo no;");
		
		return sb.toString();
				
	}
	public String getStorgeTrafficServer() {
		return storgeTrafficServer;
	}
	
	@Value("${storge.traffic.server}")
	public void setStorgeTrafficServer(String storgeTrafficServer) {
		StorgeConfig.storgeTrafficServer = storgeTrafficServer;
	}

	public String getStorgeTrafficUsername() {
		return storgeTrafficUsername;
	}

	@Value("${storge.traffic.username}")
	public void setStorgeTrafficUsername(String storgeTrafficServerUsername) {
		StorgeConfig.storgeTrafficUsername = storgeTrafficServerUsername;
	}

	public String getStorgeTrafficPassword() {
		return storgeTrafficPassword;
	}

	@Value("${storge.traffic.password}")
	public void setStorgeTrafficPassword(String storgeTrafficServerPassword) {
		StorgeConfig.storgeTrafficPassword = storgeTrafficServerPassword;
	}

	public String getStorgeAnalyticServer() {
		return storgeAnalyticServer;
	}

	@Value("${storge.analytic.server}")
	public void setStorgeAnalyticServer(String storgeAnalyticServer) {
		StorgeConfig.storgeAnalyticServer = storgeAnalyticServer;
	}

	public String getStorgeFilesServer() {
		return storgeFilesServer;
	}

	@Value("${storge.files.server}")
	public void setStorgeFilesServer(String storgeFilesServer) {
		StorgeConfig.storgeFilesServer = storgeFilesServer;
	}

}
