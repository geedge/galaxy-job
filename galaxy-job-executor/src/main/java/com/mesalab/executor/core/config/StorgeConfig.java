package com.mesalab.executor.core.config;

import com.alibaba.nacos.api.config.annotation.NacosValue;
import com.google.common.base.Splitter;
import com.zdjizhi.utils.StringUtil;
import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
@Configuration
public class StorgeConfig {

	@NacosValue(value = "${storge.traffic.server}",autoRefreshed = true)
	private String trafficServer;// clickhouse要查看的ip

	@NacosValue(value = "${storge.traffic.username}",autoRefreshed = true)
	private String trafficUsername;// clickhouse要查看的username
	
	@NacosValue(value = "${storge.traffic.password}",autoRefreshed = true)
	private String trafficPassword;// clickhouse要查看的password
	
	@NacosValue(value = "${storge.traffic.datasource}",autoRefreshed = true)
	private String trafficDatasource;// clickhouse要查看的datasource
	
	@NacosValue(value = "${storge.traffic.system.parts}",autoRefreshed = true)
	private String systemParts ;

	@NacosValue(value = "${storge.traffic.system.partsclusters}",autoRefreshed = true)
	private String systemPartsCluster ;

	@NacosValue(value = "${storge.traffic.system.disks}",autoRefreshed = true)
	private String systemDisks ;
	
	@NacosValue(value = "${storge.traffic.system.clusters}",autoRefreshed = true)
	private String systemClusters ;

	@NacosValue(value = "${storge.traffic.system.tables}",autoRefreshed = true)
	private String systemTables ;

	@NacosValue(value = "${storge.analytic.server}",autoRefreshed = true)
	private String analyticServer;// druid 节点ip和port

	@NacosValue(value = "${storge.files.hos-server}",autoRefreshed = true)
	private String filesHosServer;// 清库删除文件服务器 ip:port

	@NacosValue(value = "${storge.files.token}",autoRefreshed = true)
	private String filesToken; //登录验证

	public String getFilesHosServer() {
		return getServer();
	}

	private String getServer() {
		return getServer(filesHosServer);
	}

	public String getTrafficServer() {
		return getServer(trafficServer);
	}
	public String getAnalyticServer() {
		return getServer(analyticServer);
	}

	public Map<String,String> getFilesDataCenter() {
		return getDCMap(filesHosServer);
	}

	public Map<String,String> getTrafficDataCenter() {
		return getDCMap(trafficServer);
	}
	public Map<String,String> getAnalyticDataCenter() {
		return getDCMap(analyticServer);
	}
	private String getServer(String dcStr) {
		return Splitter.on(",").trimResults().splitToList(dcStr).stream().map(x -> x.substring(x.indexOf("|") + 1)).filter(x -> StringUtil.isNotEmpty(x)).collect(Collectors.joining(","));
	}

	private Map<String, String> getDCMap(String dcStr) {
		Map<String, String> map = new HashMap();
		Splitter.on(",").trimResults().splitToList(dcStr).stream().forEach(x -> {
			List<String> dc = Splitter.on("|").trimResults().splitToList(x);
			if (dc.size() == 2 && StringUtil.isNotBlank(dc.get(1))) {
				map.put(dc.get(0), dc.get(1));
			}
		});
		return map;
	}
}
