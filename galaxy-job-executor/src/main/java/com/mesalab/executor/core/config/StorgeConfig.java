package com.mesalab.executor.core.config;

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
	
	@Value("${storge.traffic.system.parts}")
	private String systemParts ;
	
	@Value("${storge.traffic.system.disks}")
	private String systemDisks ;
	
	@Value("${storge.traffic.system.clusters}")
	private String systemClusters ;

	@Value("${storge.traffic.system.tables}")
	private String systemTables ;


	@Value("${storge.analytic.server}")
	private String analyticServer;// druid 节点ip和port

	@Value("${storge.files.hos-server}")
	private String filesHosServer;// 清库删除文件服务器 ip:port

	@Value("${storge.files.alone.hbase-dir}")
	private String hbaseLinuxDir;// 单机数据目录在linux上的位置
	
	@Value("${storge.files.cluster.hdfs-server}")
	private String filesHdfsServer;// 存储配额集群查询文件服务器 ip:port
	
	@Value("${storge.files.cluster.hdfs-dir}")
	private String hbaseHdfsDir;// HBase 在HDFS上的路径
	
	@Value("${storge.isclusder}")
	private Boolean isClusder;// 是否为集群
	
	@Value("${storge.files.token}")
	private String filesToken; //登录验证


}
