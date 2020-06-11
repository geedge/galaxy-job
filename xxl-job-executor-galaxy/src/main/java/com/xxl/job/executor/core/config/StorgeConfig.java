package com.xxl.job.executor.core.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Data
@Configuration
public class StorgeConfig {

	@Value("${storge.traffic.server}")
	private String trafficServer;// clickhouse要查看的ip
	
	@Value("${storge.traffic.username}")
	private String trafficUsername;// clickhouse要查看的username
	
	@Value("${storge.traffic.password}")
	private String trafficPassword;// clickhouse要查看的password
	
	@Value("${storge.traffic.datasource}")
	private String trafficDatasource;// clickhouse要查看的datasource
	
	@Value("${storge.traffic.clustername}")
	private String trafficClusterName = "ck_cluster";
	
	
	
	@Value("${storge.analytic.server.select}")
	private String analyticServerSelect;// druid 查询节点ip和port
	
	@Value("${storge.analytic.server.delete}")
	private String analyticServerDelete;// druid 删除节点ip和port
	
	@Value("${storge.analytic.deletestarttime}")
	private String analyticDeleteStartTime = "2000-01-01";// druid 清库删除起始时间
	
	@Value("${storge.files.server}")
	private String filesServer;// 文件服务器 ip:port
	
	/**
	 * 获取修改存储配额脚本参数命令
	 * @param scriptDirectory 脚本所在文件绝对路径
	 * DEPLOY=yes 标识脚本的第一次部署已经没了
	 */
	public String getStorgeConfigCommand (String scriptDirectory) {
		StringBuffer sb = new StringBuffer();
		sb.append("sed -i \"s%DEPLOY=no%DEPLOY=yes%g\" "+scriptDirectory+"*.sh;");
		sb.append("sed -i \"s%BASE_DIR=.*%BASE_DIR="+scriptDirectory+"%g\" "+scriptDirectory+"*.sh;");
		sb.append("sed -i \"s%TRAFFIC_SERVER=.*%TRAFFIC_SERVER="+trafficServer+"%g\" "+scriptDirectory+"*.sh;");
		sb.append("sed -i \"s%TRAFFIC_USERNAME=.*%TRAFFIC_USERNAME="+trafficUsername+"%g\" "+scriptDirectory+"*.sh;");
		sb.append("sed -i \"s%TRAFFIC_PASSWORD=.*%TRAFFIC_PASSWORD="+trafficPassword+"%g\" "+scriptDirectory+"*.sh;");
		sb.append("sed -i \"s%ANALYTIC_SERVER=.*%ANALYTIC_SERVER="+analyticServerSelect+"%g\" "+scriptDirectory+"*.sh;");
		sb.append("sed -i \"s%FILES_SERVER=.*%FILES_SERVER="+filesServer+"%g\" "+scriptDirectory+"*.sh;");
		sb.append("chmod -R +x "+scriptDirectory+";");
		return sb.toString();
	}
	/**
	 * 判断是否已经部署过脚步，并修改了参数
	 * @return
	 */
	public String getDeployScriptCommand (String scriptDirectory) {
		StringBuffer sb = new StringBuffer();
		sb.append("cat "+scriptDirectory+"*.sh|grep DEPLOY=yes||echo no;");
		
		return sb.toString();
				
	}
}
